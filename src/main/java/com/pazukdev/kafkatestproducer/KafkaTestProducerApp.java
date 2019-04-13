package com.pazukdev.kafkatestproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Scanner;

@SpringBootApplication
public class KafkaTestProducerApp implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger("KafkaTestProducerApp");

    @Value("${message.topic.name}")
    private String defaultTopic;
    private String selectedTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaTestProducerApp(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestProducerApp.class, args);
    }

    @Override
    public void run(String... strings) {
        processInput();
    }

    private void processInput() {
        selectedTopic = defaultTopic;
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
            final String message = scanner.nextLine();

            if (messageIsCommandToSelectTopic(message)) {
                selectedTopic = getTopicFromCommand(message);
                LOG.info("selectedTopic: " + selectedTopic);
            } else {
                kafkaTemplate.send(selectedTopic, message);
                LOG.info("Published message to topic: {}.", selectedTopic);
            }
        }
    }

    private boolean messageIsCommandToSelectTopic(final String message) {
        return message.contains("select topic:");
    }

    private String getTopicFromCommand(final String command) {
        return command.split(": ")[1];
    }

}




























