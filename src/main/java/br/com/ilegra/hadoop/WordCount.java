package br.com.ilegra.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Uso: WordCount <entrada> <saida>");
			System.exit(2);
		}

		Job job = setConfiguration();
		setMapperAndReducer(job);
		setTypeOutput(job);
		setKeyValueOutput(job);
		setInputOutputHDFS(args, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static Job setConfiguration() throws IOException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");
		return job;
	}

	private static void setMapperAndReducer(Job job) {
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
	}

	private static void setTypeOutput(Job job) {
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
	}

	private static void setKeyValueOutput(Job job) {
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	}

	private static void setInputOutputHDFS(String[] args, Job job)
			throws IOException {
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	}
}
