package com.googlecloud.storage;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CloudStorage {

    private Storage storage;
    private Bucket bucket;
    private static String BUCKET_NAME = "quest-bucket";
    private static String FIRST_BLOB_NAME = "first-blob";
    private static String SECOND_BLOB_NAME = "second-blob";

    private static String PROJECT_ID = "quest-cloud-tutorial";
    private static String SUBSCRIPTION_ID = "projects/quest-cloud-tutorial/subscriptions/topic-one-subscription";
    private static String TOPIC_ONE = "topic-one";

    private CloudStorage() {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    public static void main(String[] args) throws Exception {
        CloudStorage cloudStorage = new CloudStorage();
        Bucket bucket = cloudStorage.getBucket(BUCKET_NAME);
        BlobId blFirst = cloudStorage.saveStringToBucket(FIRST_BLOB_NAME, "Hi there Lalit. Data is inserted in GCP bucket!", bucket);
        BlobId blSecond = cloudStorage.saveStringToBucket(SECOND_BLOB_NAME, "Bye From GCP bucket!", bucket);
        String valueById = cloudStorage.getStringFromBucketById(blFirst);
        System.out.println("Value printed by using bucket Id : " + valueById);

        String valueByName = cloudStorage.getStringFromBucketByName(SECOND_BLOB_NAME);
        System.out.println("Value printed by using bucket Name : " + valueByName);

        cloudStorage.updateDataInsideBucket(blSecond, "Data Updated ... Bye now!");
        String valueByNameAfterUpation = cloudStorage.getStringFromBucketByName(SECOND_BLOB_NAME);
        System.out.println("Value printed by using bucket Name after updation: " + valueByNameAfterUpation);

        cloudStorage.fetchAndCreateTopic(PROJECT_ID);
        cloudStorage.createPullSubscription(PROJECT_ID,SUBSCRIPTION_ID,TOPIC_ONE);
        cloudStorage.pushDataToPubSubFromGCS(PROJECT_ID, TOPIC_ONE, BUCKET_NAME);
        cloudStorage.readDataFromPubSubUsingPullSubscription(PROJECT_ID, "topic-one-subscription");

    }

    private Bucket getBucket(String bucketName) {
        bucket = storage.get(bucketName);
        if (bucket == null) {
            System.out.println("Creating new bucket.");
            bucket = storage.create(BucketInfo.of(bucketName));
            System.out.printf("Bucket %s created.%n", bucket.getName());
        }
        return bucket;
    }

    // Save a string to a blob inside bucket
    private BlobId saveStringToBucket(String blobName, String value, Bucket bucket) {
        byte[] bytes = value.getBytes(UTF_8);
        Blob blob = bucket.create(blobName, bytes);
        return blob.getBlobId();
    }

    // Fetch data from bucket by using Id
    private String getStringFromBucketById(BlobId blobId) {
        Blob blob = storage.get(blobId);
        return new String(blob.getContent());
    }

    // get a blob by name
    private String getStringFromBucketByName(String name) {
        Page<Blob> blobs = bucket.list();
        for (Blob blob : blobs.getValues()) {
            if (name.equals(blob.getName())) {
                return new String(blob.getContent());
            }
        }
        return "There is no blob found in the given name...";
    }

    // Updating data inside a blob
    private void updateDataInsideBucket(BlobId blobId, String newString) throws IOException {
        Blob blob = storage.get(blobId);
        if (blob != null) {
            WritableByteChannel channel = blob.writer();
            channel.write(ByteBuffer.wrap(newString.getBytes(UTF_8)));
            channel.close();
        }
    }

    private void createTopic(String projectId, String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: " + topic.getName());
        }
    }

    private void fetchAndCreateTopic(String projectId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            Iterable<Topic> topics = topicAdminClient.listTopics(projectName).iterateAll();
            if (!topics.iterator().hasNext()) {
                createTopic("quest-cloud-tutorial", "topic-one");
            }
        }
    }

    private void pushDataToPubSubFromGCS(String projectId, String topicId, String bucketName) throws IOException, InterruptedException, ExecutionException {

        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();
            Page<Blob> blobData = storage.list(bucketName);
            for (Blob blob : blobData.iterateAll()) {
                byte[] content = blob.getContent();
                ByteString data = ByteString.copyFrom(content);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);

                String messageId = messageIdFuture.get();
                System.out.println("Published message id is: " + messageId);
            }
        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
        System.out.println("Message published successfully to PUB-SUB from GCS");
    }

    public static void createPullSubscription(String projectId, String subscriptionId, String topicId) throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            Subscription subscription = null;
            if(subscriptionAdminClient.getSubscription(subscriptionId).getName() == null){
                subscription = subscriptionAdminClient.createSubscription(subscriptionId, topicName, PushConfig.getDefaultInstance(), 10);
                System.out.println("Created pull subscription: " + subscription.getName());
            }
        }
    }

    private void readDataFromPubSubUsingPullSubscription(String projectId, String createdSubscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, createdSubscriptionId);

        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            // Handle incoming message, then ack the received message.
            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            // Print message attributes.
            message.getAttributesMap().forEach((key, value) -> System.out.println(key + " = " + value));
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            // Start the subscriber.
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            subscriber.stopAsync();
        }
    }
}
