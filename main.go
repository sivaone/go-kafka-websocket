package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"time"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "notifygrp",
		"auto.offset.reset": "smallest"})

	if err != nil {
		panic(err)
	}

	consumer.Subscribe("realtimealerts", nil)
	defer consumer.Close()
	run := true

	for run == true {

		msg, err := consumer.ReadMessage(2 * time.Second)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			err := wsConn.WriteMessage(websocket.TextMessage, msg.Value)
			if err != nil {
				log.Println("unable to write message")
			}
		}

		time.Sleep(1 * time.Second)
	}

}

func handleNotifications(c *gin.Context) {
	wsHandler(c.Writer, c.Request)
}

func main() {
	router := gin.Default()
	router.GET("/wsnotif", handleNotifications)

	router.Run("localhost:8080")
}
