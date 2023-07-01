package GoFastEvent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
)

type EventConfig struct {
	Channel          *amqp091.Channel
	ConnectionString string
	ExchangeName     string
	QueueName        string
}

type EventServer struct {
	Config        EventConfig
	EventHandlers map[string]func(delivery amqp091.Delivery)
	RunState      *sync.WaitGroup
}

func (e *EventServer) Initialize() {
	failOnError := func(err error, msg string) {
		if err != nil {
			log.Println(fmt.Sprintf("%s : %s", msg, err))
			e.RunState.Done()
		}
	}

	go func() {
		conn, err := amqp091.Dial(e.Config.ConnectionString)
		failOnError(err, "failed to dial")
		defer conn.Close()

		ch, err := conn.Channel()
		failOnError(err, "failed initializing channel")
		if e.Config.Channel == nil {
			e.Config.Channel = ch
		}
		defer ch.Close()

		log.Println("channel initiated")

		err = channelDeclareQueue(e.Config.QueueName, ch)
		failOnError(err, "failed to declare queue")
		err = channelDeclareExchange(e.Config.ExchangeName, ch)
		failOnError(err, "failed to declare exchange")
		err = channelBindQueue(e.Config.QueueName, e.Config.ExchangeName, ch)
		failOnError(err, "failed to bind queue")

		eventHandlers := e.EventHandlers
		for {
			messages, _ := channelConsume(e.Config.QueueName, ch)
			for msg := range messages {
				var evtMsg EventMessage
				err := json.Unmarshal(msg.Body, &evtMsg)
				if err != nil {
					log.Println("error json-serializing event message")
				}

				if _, ok := eventHandlers[evtMsg.EventName]; ok {
					log.Println(fmt.Sprintf("Event Received with Valid Handler Name: %s", evtMsg.Message))
					eventHandlers[evtMsg.EventName](msg)
				} else {
					log.Println(fmt.Sprintf("No event handlers found w/ name: %s", evtMsg.EventName))
					err = msg.Ack(false)
					if err != nil {
						log.Println("error acknowledging event: " + err.Error())
					}
				}
			}
		}
	}()
}

func channelDeclareQueue(queueName string, ch *amqp091.Channel) error {
	_, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil)
	return err
}

func channelDeclareExchange(exchangeName string, ch *amqp091.Channel) error {
	err := ch.ExchangeDeclare(
		exchangeName,
		amqp091.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil)
	return err
}

func channelBindQueue(queueName, exchangeName string, ch *amqp091.Channel) error {
	err := ch.QueueBind(
		queueName,
		queueName+"-routingKey",
		exchangeName,
		false,
		nil)
	return err
}

func channelConsume(queueName string, ch *amqp091.Channel) (<-chan amqp091.Delivery, error) {
	return ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil)
}

type EventMessage struct {
	EventName string `json:"eventName"`
	Message   string `json:"message"`
}

type BaseEventHandler[T interface{}] struct {
	ExecutorFunc func(ctx context.Context, data T)
}

func (b *BaseEventHandler[T]) GetHandler() func(d amqp091.Delivery) {
	return func(d amqp091.Delivery) {
		data, err := parseEventMessage[T](d)
		if err != nil {
			log.Println("error parsing amqp data!")

			err = d.Ack(false)
			if err != nil {
				log.Println("parse amqp data failed -> acknowledgement failed!")
			}
			return
		}
		ctx := context.Background()
		b.ExecutorFunc(ctx, data)

		err = d.Ack(false)
		if err != nil {
			log.Println("event w/ valid event handler name ack failed")
		}
	}
}

func parseEventMessage[T interface{}](d amqp091.Delivery) (T, error) {
	var eventMessage EventMessage
	err := json.Unmarshal(d.Body, &eventMessage)
	if err != nil {
		log.Println("error unmarshalling event message!")
		return *new(T), err
	}

	var data T
	err = json.Unmarshal([]byte(eventMessage.Message), &data)
	if err != nil {
		log.Println("error unmarshalling message")
		return *new(T), err
	}

	return data, nil
}

type EventPublishRequest struct {
	Context      context.Context
	Channel      *amqp091.Channel
	Message      string
	EventName    string
	QueueName    string
	ExchangeName string
}

func (e *EventPublishRequest) Publish() error {
	bytesOfData, err := json.Marshal(e.Message)
	if err != nil {
		log.Println("failed to marshal data")
		return err
	}

	eventMessage := EventMessage{
		Message:   string(bytesOfData),
		EventName: e.EventName,
	}
	bytesOfEventMessage, err := json.Marshal(eventMessage)
	if err != nil {
		log.Println("failed to marshal event message")
		return err
	}

	err = e.Channel.PublishWithContext(
		e.Context,
		e.ExchangeName,
		e.QueueName+"-routingKey",
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        bytesOfEventMessage,
		})
	if err != nil {
		log.Println("failed to publish")
		return err
	}

	return nil
}
