import argparse
import json
import logging
import os
import math

import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from kafka import KafkaProducer

def singleton(cls):
  instances = {}
  def getinstance(*args, **kwargs):
    if cls not in instances:
      instances[cls] = cls(*args, **kwargs)
    return instances[cls]
  return getinstance

class Filter(beam.DoFn):

    def process(self, element):
        for key,value in element.items():
            if value is None:
                return
            else:
                yield element

class Convert(beam.DoFn):

    def process(self, element):
        for key, value in element.items():
            if key == "temperature":
                f = (value * 1.8) + 32
                return f
            if key == "pressure":
                return value/6.895

class RiskEstimation(beam.DoFn):

    def process(self, element):
        temp = element["temperature"]
        humd = element["humdidity"]
        pres = element["pressure"]

        if humd > 90:
            element["risk"] = 1
        if pres >=20 and humd <=90:
            element["risk"] = 1-math.pow( 2.718281828, 0.2-pres)
        if pres <20 and humd <= 90:
            element["risk"] = min(1, temp/300)
        return element

class ProduceKafkaMessage(beam.DoFn):

    def __init__(self, topic, servers, *args, **kwargs):
        beam.DoFn.__init__(self, *args, **kwargs)
        self.topic=topic;
        self.servers=servers;

    def start_bundle(self):
        self._producer = KafkaProducer(**self.servers)

    def finish_bundle(self):
        self._producer.close()

    def process(self, element):
        try:
            self._producer.send(self.topic, element[1], key=element[0])
            yield element
        except Exception as e:
            raise