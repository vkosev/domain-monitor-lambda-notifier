package com.notifier;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NotificationHandler implements RequestHandler<Object, String> {

    private static final String     TOPIC_ARN = System.getenv("SNS_TOPIC_ARN");

    private static final Logger     LOGGER = LoggerFactory.getLogger(NotificationHandler.class);

    private final        SnsClient  snsClient = SnsClient.create();

    @Override
    public String handleRequest(Object input, Context context) {
        LOGGER.info("Expiration Check Lambda Triggered");

        List<ExpirationCheck> expiredDomains = checkForExpiration();
        if(expiredDomains == null) {
            LOGGER.error("Failed to query records");

            return "{\"error\": \"" + "Failed to query database"  + "\"}";
        }

        List<ExpirationCheck> uniqueExpirations = new ArrayList<>();

        for (ExpirationCheck expiration : expiredDomains) {
            if(!uniqueExpirations.contains(expiration)) {
                uniqueExpirations.add(expiration);
            }
        }

        if (!uniqueExpirations.isEmpty()) {
            LOGGER.error("Expired Domains Found! Sending notifications!");

            try {
                String expiredDomainNames =uniqueExpirations.stream().map(ExpirationCheck::domain).collect(Collectors.joining(","));

                PublishRequest publishRequest = PublishRequest.builder()
                        .topicArn(TOPIC_ARN)
                        .message(expiredDomainNames)
                        .subject("Expired Domains Alert")
                        .build();

                snsClient.publish(publishRequest);

                return "{\"status\": \"SNS notification sent\", \"count\": " + expiredDomains.size() + "}";
            } catch (Exception e) {
                LOGGER.error("Failed to send message to SNS ", e);

                return "{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}";
            }
        }

        LOGGER.info("No Expired Domains Found!");

        return "{\"status\": \"All domains are valid\"}";
    }

    private List<ExpirationCheck> checkForExpiration() {

        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");
        String jdbcUrl = System.getenv("DB_URL");

        List<ExpirationCheck> expiredDomains = new ArrayList<>();

        String query = "SELECT id, domain, expiration_date FROM expiration_check_records WHERE is_expired = true";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String domain = rs.getString("domain");

                expiredDomains.add(new ExpirationCheck(domain));
            }

        } catch (Exception e) {
            LOGGER.error("Exception while querying records: ", e);
            return null;
        }

        return expiredDomains;
    }

    public record ExpirationCheck(String domain) {

    }
}