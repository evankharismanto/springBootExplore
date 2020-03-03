package com.backend.spring.rsocket.server.controller;

import com.backend.spring.rsocket.server.data.Message;
import lombok.extern.log4j.Log4j2;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;

@Log4j2
@Controller
public class RSocketController {

    private static final String SERVER = "Server";
    private static final String RESPONSE = "Response";
    private static final String STREAM = "Stream";
    private static final String CHANNEL = "Channel";

    @MessageMapping("request-response")
    Message requestResponse(Message request) {
        log.info("Received request-response request: {}", request);

        return new Message(SERVER, RESPONSE);
    }

    @MessageMapping("fire-and-forget")
    public void fireAndForget(Message request) {
        log.info("Received fire-and-forget request: {}", request);

        return;
    }

    @MessageMapping("stream")
    Flux<Message> stream(Message request) {
        log.info("Received stream request: {}", request);
        return Flux
                // create a new Flux emitting an element every 1 second
                .interval(Duration.ofSeconds(1))
                // index the Flux
                .index()
                // create a Flux of new Messages using the indexed Flux
                .map(objects -> new Message(SERVER, STREAM, objects.getT1()))
                // use the Flux logger to output each flux event
                .log();
    }

    @MessageMapping("channel")
    Flux<Message> channel(Flux<Message> requests) {
        log.info("Received channel request (stream) at {}", Instant.now());
        return requests
                // Create an indexed flux which gives each element a number
                .index()
                // log what has been received
                .log()
                // then every 1 second per element received
                .delayElements(Duration.ofSeconds(1))
                // create a new Flux with one Message for each element (numbered)
                .map(objects -> new Message(SERVER, CHANNEL, objects.getT1()))
                // log what is being sent
                .log();
    }
}
