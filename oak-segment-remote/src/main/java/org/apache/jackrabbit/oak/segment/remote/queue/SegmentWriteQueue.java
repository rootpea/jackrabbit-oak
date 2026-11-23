/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.remote.queue;

import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SegmentWriteQueue implements Closeable {

    public static final int THREADS = Integer.getInteger("oak.segment.remote.threads", 5);

    private static final int QUEUE_SIZE = Integer.getInteger("oak.segment.remote.queue.size", 20);

    private static final Logger log = LoggerFactory.getLogger(SegmentWriteQueue.class);

    private final BlockingDeque<SegmentWriteAction> queue;

    private final BlockingDeque<SegmentWriteAction> retryQueue;

    private final Map<UUID, SegmentWriteAction> segmentsByUUID;

    private final Set<UUID> failedSegments;

    private final ExecutorService executor;

    private final ReadWriteLock flushLock;

    private final SegmentConsumer writer;

    private volatile boolean shutdown;

    private final Object brokenMonitor = new Object();

    private volatile boolean broken;

    public SegmentWriteQueue(SegmentConsumer writer) {
        this(writer, QUEUE_SIZE, THREADS);
    }

    SegmentWriteQueue(SegmentConsumer writer, int queueSize, int threadNo) {
        this.writer = writer;
        segmentsByUUID = new ConcurrentHashMap<>();
        failedSegments = new HashSet<>();
        flushLock = new ReentrantReadWriteLock();

        queue = new LinkedBlockingDeque<>(queueSize);
        retryQueue = new LinkedBlockingDeque<>();
        executor = Executors.newFixedThreadPool(threadNo + 1);
        for (int i = 0; i < threadNo; i++) {
            executor.submit(this::mainLoop);
        }
        executor.submit(this::emergencyLoop);
    }

    private void mainLoop() {
        while (!shutdown) {
            try {
                waitWhileBroken();
                if (shutdown) {
                    break;
                }
                consume();
            } catch (SegmentConsumeException e) {
                SegmentWriteAction segment = e.segment;
                log.error("Can't persist the segment {}", segment.getUuid(), e.getCause());
                retryQueue.offerLast(segment);
            }
        }
    }

    private void consume() throws SegmentConsumeException {
        SegmentWriteAction segment = retryQueue.pollFirst();
        try {
            if (segment == null) {
                segment = queue.poll(100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            log.error("Poll from queue interrupted", e);
        }
        if (segment != null) {
            consume(segment);
        }
    }

    private void consume(SegmentWriteAction segment) throws SegmentConsumeException {
        try {
            segment.passTo(writer);
        } catch (IOException | RuntimeException e) {
            setBroken(segment, true);
            throw new SegmentConsumeException(segment, e);
        }
        setBroken(segment, false);
        synchronized (segmentsByUUID) {
            segmentsByUUID.remove(segment.getUuid());
            segmentsByUUID.notifyAll();
        }
    }

    private void emergencyLoop() {
        while (!shutdown) {
            waitUntilBroken();
            if (shutdown) {
                break;
            }

            boolean success = false;
            SegmentWriteAction segmentToRetry = null;
            do {
                try {
                    if (segmentToRetry == null) {
                        consume();
                    } else {
                        consume(segmentToRetry);
                    }
                    success = true;
                } catch (SegmentConsumeException e) {
                    segmentToRetry = e.segment;
                    log.error("Can't persist the segment {}", segmentToRetry.getUuid(), e.getCause());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        log.warn("Interrupted", e);
                    }
                    if (shutdown) {
                        log.error("Shutdown initiated. The segment {} will be dropped.", segmentToRetry.getUuid());
                    }
                }
            } while (!success && !shutdown);
        }
    }

    public void addToQueue(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        waitWhileBroken();
        if (shutdown) {
            throw new IllegalStateException("Can't accept the new segment - shutdown in progress");
        }

        SegmentWriteAction action = new SegmentWriteAction(indexEntry, data, offset, size);
        flushLock.readLock().lock();
        try {
            segmentsByUUID.put(action.getUuid(), action);
            if (!queue.offer(action, 1, TimeUnit.MINUTES)) {
                segmentsByUUID.remove(action.getUuid());
                throw new IOException("Can't add segment to the queue");
            }
        } catch (InterruptedException e) {
            segmentsByUUID.remove(action.getUuid());
            throw new IOException(e);
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public void flush() throws IOException {
        flushLock.writeLock().lock();
        try {
            synchronized (segmentsByUUID) {
                long start = System.currentTimeMillis();
                while (!segmentsByUUID.isEmpty()) {
                    segmentsByUUID.wait(100);
                    if (System.currentTimeMillis() - start > TimeUnit.MINUTES.toMillis(1)) {
                        log.error("Can't flush the queue in 1 minute. Queue: {}. Segment map: {}", queue, segmentsByUUID);
                        start = System.currentTimeMillis();
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    public SegmentWriteAction read(UUID id) {
        return segmentsByUUID.get(id);
    }

    @Override
    public void close() throws IOException {
        shutdown = true;
        try {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new IOException("The write wasn't able to shut down clearly");
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public boolean isEmpty() {
        return segmentsByUUID.isEmpty();
    }

    boolean isBroken() {
        return broken;
    }

    int getFailedCount() {
        synchronized (brokenMonitor) {
            return failedSegments.size();
        }
    }

    int getSize() {
        return queue.size() + retryQueue.size();
    }

    private void setBroken(SegmentWriteAction segment, boolean failed) {
        synchronized (brokenMonitor) {
            if (failed) {
                failedSegments.add(segment.getUuid());
            } else {
                failedSegments.remove(segment.getUuid());
            }
            broken = !failedSegments.isEmpty();
            brokenMonitor.notifyAll();
        }
    }

    private void waitWhileBroken() {
        if (!broken) {
            return;
        }
        synchronized (brokenMonitor) {
            while (broken && !shutdown) {
                try {
                    brokenMonitor.wait(100);
                } catch (InterruptedException e) {
                    log.warn("Interrupted", e);
                }
            }
        }
    }

    private void waitUntilBroken() {
        if (broken) {
            return;
        }
        synchronized (brokenMonitor) {
            while (!broken && !shutdown) {
                try {
                    brokenMonitor.wait(100);
                } catch (InterruptedException e) {
                    log.warn("Interrupted", e);
                }
            }
        }
    }

    public interface SegmentConsumer {

        void consume(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException;

    }

    public static class SegmentConsumeException extends Exception {

        private final SegmentWriteAction segment;

        public SegmentConsumeException(SegmentWriteAction segment, Exception cause) {
            super(cause);
            this.segment = segment;
        }
    }
}
