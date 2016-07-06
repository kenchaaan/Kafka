package io.vertx.example;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.vertx.core.Vertx;


/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class HelloWorldEmbedded {

  public static void main(String[] args) {
    // Create an HTTP server which simply returns "Hello World!" to each request.
    //Vertx.vertx().createHttpServer().requestHandler(req -> req.response().end("Hello World!")).listen(8080);
  

	  Properties props = new Properties();
      props.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put("bootstrap.servers","localhost:9092");
      props.put("acks","all");
//      props.put("block.on.buffer.full","true");
      props.put("batch.size","1");
//      props.put("security.protocol","SASL_SSL");
//      props.put("ssl.protocol","TLSv1.2");
//      props.put("ssl.enabled.protocols","TLSv1.2");
//      props.put("ssl.truststore.location","/usr/lib/j2re1.7-ibm/jre/lib/security/cacerts");
//      props.put("ssl.truststore.password","password");
//      props.put("ssl.truststore.type","JKS");
//      props.put("ssl.endpoint.identification.algorithm","HTTPS");
  
  
      String topic="test";
      String message="publish message to a topic";
      try {
        KafkaProducer kafkaProducer;
        kafkaProducer = new KafkaProducer(props);
        for (int i = 0; i < 1000; i++){
        	message = message + ": " + Integer.toString(i);
	        ProducerRecord producerRecord = new ProducerRecord(topic,message.getBytes("UTF-8"));
	        RecordMetadata recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
        }
        //getting RecordMetadata is possible to validate topic, partition and offset
//        System.out.println("topic where message is published : " + recordMetadata.topic());
//        System.out.println("partition where message is published : " + recordMetadata.partition());
//        System.out.println("message offset # : " + recordMetadata.offset());
          //kafkaProducer.send(producerRecord);
          System.out.println("message sent");
        kafkaProducer.close();
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }   
  }
}
