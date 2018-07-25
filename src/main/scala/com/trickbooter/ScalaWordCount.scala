package com.trickbooter

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.{Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory

object ScalaWordCount {

  val animals = "Echidna Echidna Koala Wombat Marmot Quokka Kangaroo Dingo Numbat Emu Wallaby CaneToad Bilby Possum Cassowary Kookaburra Platypus Bandicoot Cockatoo Antechinus"

  private val log = LoggerFactory.getLogger(ScalaWordCount.getClass)

  trait Options extends PipelineOptions {
    @Description("Output path")
    def getOutput: String

    def setOutput(value: String): Unit
  }

  def extractWords = new DoFn[String, String] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      for (word <- c.element().split("[^\\p{L}]+")) yield {
        if (!word.isEmpty) c.output(word)
      }
    }
  }

  def formatResults = new SimpleFunction[KV[String, java.lang.Long], String] {
    override def apply(input: KV[String, java.lang.Long]): String = input.getKey + ": " + input.getValue
  }

  def boundedFlow(args: Array[String]): Pipeline = {
    val options = PipelineOptionsFactory.fromArgs(args: _ *).withValidation.as(classOf[ScalaWordCount.Options])
    val p = Pipeline.create(options)

    // prepare flow
    val a = p
      .apply(Create.of((0 to 99).foldLeft("")((a, b) => a + " " + animals)))
      .apply("SplitLines", ParDo.of(extractWords))
      .apply("CountWords", Count.perElement())
      .apply("FormatResults", MapElements.via(formatResults))
      .apply("WriteWords", TextIO.write().to(options.getOutput))

    p
  }

  def main(args: Array[String]): Unit = {
    //windowedFlow(args).run.waitUntilFinish
    boundedFlow(args).run.waitUntilFinish()
  }

}
