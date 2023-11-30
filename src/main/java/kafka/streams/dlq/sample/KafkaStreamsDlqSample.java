/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.streams.dlq.sample;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.StreamRetryTemplate;
import org.springframework.cloud.stream.binder.kafka.ListenerContainerWithDlqAndRetryCustomizer;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;

import org.springframework.cloud.stream.binder.kafka.streams.DltAwareProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.DltPublishingContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.lang.Nullable;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.apache.kafka.streams.processor.api.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.backoff.BackOff;

@SpringBootApplication
public class KafkaStreamsDlqSample {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsDlqSample.class, args);
	}

	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsDlqSample.class);

	@Autowired
	private StreamBridge streamBridge;

	@Autowired
	private DltPublishingContext dltPublishingContext;

	public static class WordCountProcessorApplication {

		Predicate<Object, WordCount> a = (k, v) -> v.word.startsWith("a");
		Predicate<Object, WordCount> b =  (k, v) -> v.word.startsWith("b");
		Predicate<Object, WordCount> c = (k, v) -> v.word.startsWith("c");

		@Bean
		public Function<KStream<Object, String>,KStream<?, WordCount>[]> process( DltPublishingContext dltPublishingContext,
																				  @Lazy @Qualifier("process-in-0-RetryTemplate")  RetryTemplate retryTemplate) {

			return input -> {
				final Map<String, KStream<Object, WordCount>> stringKStreamMap =

						input
						//.process( new TopicPartitionProcessorSupplier() )
						.process( () ->  {
								return new DltAwareProcessor<>(
										record -> {

											LOG.info("Processing " + record.value());

											return retryTemplate.execute(context -> {

												LOG.info("In retryTemplate Processing " + record.value());


												if (record.value().equals("ABC")) {
													LOG.info("Going to throw RuntimeException !");
													throw new RuntimeException("error");
												}

												return record;
											});

										},
										"words-count-dlq", dltPublishingContext);

						})
						.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
						.map((key, value) -> new KeyValue<>(value, value))
						.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
						.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30) ))
						.count(Materialized.as("WordCounts-1"))
						.toStream()
						.map((key, value) -> new KeyValue<>(null,
								new WordCount( key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
						.split()
								.branch(a)
								.branch(b)
								.branch(c)
								.defaultBranch();

				return stringKStreamMap.values().toArray(new KStream[0]);
			};

		}

		@Bean
		@StreamRetryTemplate
		RetryTemplate myRetryTemplate() {
			RetryTemplate retryTemplate = new RetryTemplate();

			RetryPolicy retryPolicy = new SimpleRetryPolicy(4);
			FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
			backOffPolicy.setBackOffPeriod(1);

			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryTemplate.setRetryPolicy(retryPolicy);

			return retryTemplate;
		}

		public static class TopicPartitionProcessorSupplier implements ProcessorSupplier<Object, String,Object, String>{
			@Override
			public Processor<Object, String,Object, String> get() {
				return new TopicPartitionProcessor();
			}
		}

		public static class TopicPartitionProcessor implements Processor<Object, String,Object, String> {
			private ProcessorContext<Object, String> context;

			@Override
			public void init(ProcessorContext<Object, String> context) {
				this.context = context;
			}

			@Override
			public void process(Record<Object,String> record) {
				String topic = context.recordMetadata().get().topic();
				int partition = context.recordMetadata().get().partition();

				// Now you have the topic and partition information
				LOG.info("Topic: " + topic + ", Partition: " + partition);

				// Your processing logic here

				// Forward the processed record to the downstream
				context.forward( record );
			}

			@Override
			public void close() {
				// Cleanup logic
			}
		}

//		@Bean
//		public DlqPartitionFunction partitionFunction() {
//			return (group, record, ex) -> 0;
//		}

//		@Bean
//		ListenerContainerWithDlqAndRetryCustomizer cust(KafkaTemplate<?, ?> template) {
//			return new ListenerContainerWithDlqAndRetryCustomizer() {
//
//				@Override
//				public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName,
//									  String group,
//									  @Nullable BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> dlqDestinationResolver,
//									  @Nullable BackOff backOff) {
//
//					// Always send to first/only partition of DLT suffixed topic
//					BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>  newDlqDestinationResolver =
//							(cr, e) -> {
//
//									TopicPartition topicPartition = dlqDestinationResolver.apply(cr, e);
//
//									return new TopicPartition(topicPartition.topic(), 0);
//								};
//
//					//if (destinationName.equals("topicXYZ")) {
//						ConsumerRecordRecoverer dlpr = new DeadLetterPublishingRecoverer(template,	newDlqDestinationResolver);
//
//						container.setCommonErrorHandler(new DefaultErrorHandler(dlpr, backOff));
//					//}
//				}
//
//				@Override
//				public boolean retryAndDlqInBinding(String destinationName, String group) {
//					return !destinationName.contains("topicWithLongTotalRetryConfig");
//				}
//
//			};
//		}
	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}
}
