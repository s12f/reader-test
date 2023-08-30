package org.example;

import io.hstream.HStreamClient;
import io.hstream.StreamShardOffset;

import java.util.LinkedList;
import java.util.UUID;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");
        var client = HStreamClient.builder().serviceUrl("hstream://localhost:6570").build();
        var stream = "stream32";
        var shards = client.listShards(stream);
        var ts = new LinkedList<Thread>();
        for (var shard : shards) {
            var thread = new Thread(() -> {
                try {
                    var reader = client.newReader().streamName(stream)
                            .readerId("r_" + UUID.randomUUID().toString().replace("-", "_"))
                            .timeoutMs(1000)
                            .shardId(shard.getShardId())
                            .shardOffset(new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST))
                            .build();
                    while (true) {
                        reader.read(1).join();
                    }
                } catch (Exception e) {
                    System.out.println("shard: " + shard.getShardId());
                    e.printStackTrace();
                }
            });
            thread.start();
            ts.add(thread);
        }
        for (var t : ts) {
            t.join();
        }
    }
}