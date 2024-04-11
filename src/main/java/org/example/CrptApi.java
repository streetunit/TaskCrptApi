package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CrptApi {
    private final static long UPDATE_PERIOD = 1;
    private final String apiUrl = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final Semaphore semaphore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("requestLimit cannot be negative or 0");
        }

        semaphore = new Semaphore(requestLimit);
        scheduler.scheduleAtFixedRate(this::releasePermits, UPDATE_PERIOD, UPDATE_PERIOD, timeUnit);
    }

    public void createDocument(Object document, String signature) {
        getPermit();

        HttpRequest request = buildRequest(document, signature);

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::statusCode)
                .thenAccept(System.out::println)
                .join();
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    private HttpRequest buildRequest(Object document, String signature) {
        String jsonDoc = getObjectAsJsonString(document);

        return HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + signature)
                .POST(HttpRequest.BodyPublishers.ofString(jsonDoc))
                .build();
    }

    private void getPermit() {
        try {
            semaphore.acquire();
            requestCount.incrementAndGet();
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread was interrupted");
        }
    }

    private void releasePermits() {
        int countPermitsToRelease = requestCount.getAndSet(0);
        semaphore.release(countPermitsToRelease);
    }

    private String getObjectAsJsonString(Object object) {
        String jsonDoc;
        try {
            jsonDoc = objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Object serialization error in JSON");
        }
        return jsonDoc;
    }

    @Data
    @NoArgsConstructor
    public static class Document {
        private Description description;
        @JsonProperty("doc_id")
        private String docId;
        @JsonProperty("doc_status")
        private String docStatus;
        @JsonProperty("doc_type")
        private String docType;
        private boolean importRequest;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("participant_inn")
        private String participantInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("production_type")
        private String productionType;
        private List<Product> products;
        @JsonProperty("reg_date")
        private String regDate;
        @JsonProperty("reg_number")
        private String regNumber;
    }

    @Data
    @NoArgsConstructor
    public static class Description {
        private String participantInn;
    }

    @Data
    @NoArgsConstructor
    public static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;
        @JsonProperty("certificate_document_date")
        private String certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("tnved_code")
        private String tnvedCode;
        @JsonProperty("uit_code")
        private String uitCode;
        @JsonProperty("uitu_code")
        private String uituCode;
    }
}
