import express from "express";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();

const app = express();
const PORT = process.env.PORT;

// -------------------------------
//  ENABLE CORS WITH ENV ORIGIN
// -------------------------------
app.use(
  cors({
    origin: process.env.CORS_ORIGIN, // e.g. "http://localhost:5173"
    methods: ["GET"],
    credentials: false,
  })
);

// -------------------------------
//  WEATHER REDUCER (EXP–3)
// -------------------------------
const weatherReducerCode = `
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer; 
import java.io.IOException;

public class WeatherReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String weatherCondition = null;
        for (Text val : values) {
            weatherCondition = val.toString();
        }
        context.write(key, new Text(weatherCondition));
    }
}
`;

// -------------------------------
//  MATRIX (EXP–2)
// -------------------------------
const matrixMapperCode = `
package org.experiment2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper; 
import java.io.IOException;

public class Matrix_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\\\\s+"); 
        String matrix = parts[0];
        int i = Integer.parseInt(parts[1]); 
        int j = Integer.parseInt(parts[2]);
        double val = Double.parseDouble(parts[3]);

        int n = Integer.parseInt(context.getConfiguration().get("n"));
        int m = Integer.parseInt(context.getConfiguration().get("m"));

        if (matrix.equals("A")) {
            for (int k = 0; k < n; k++) {
                context.write(new Text(i + "," + k), new Text("A," + j + "," + val));
            }
        } else {
            for (int k = 0; k < m; k++) {
                context.write(new Text(k + "," + j), new Text("B," + i + "," + val));
            }
        }
    }
}
`;

// -------------------------------
//  TAG MAPPER (EXP–4)
// -------------------------------
const tagMapperCode = `
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
import java.io.IOException;

public class TagMapper extends Mapper<Object, Text, Text, Text> { 
    private Text movieID = new Text();
    private Text tag = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\\\\t");

        if (parts.length == 3) { 
            movieID.set(parts[0]); 
            tag.set(parts[1]); 
            context.write(movieID, tag);
        }
    }
}
`;

// -------------------------------
//  WORD COUNT MAPPER (EXP–8)
// -------------------------------
const wordCountMapperCode = `
package org.experiment8;

import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase 
        implements Mapper<LongWritable,Text,Text,IntWritable>{

    private final static IntWritable one = new IntWritable(1); 
    private Text word = new Text();

    public void map(LongWritable key, Text value,
                    OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException {

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            output.collect(word, one);
        }
    }
}
`;

// -------------------------------
//  ROUTES FOR DOWNLOAD
// -------------------------------
app.get("/download/weatherReducer", (req, res) => {
  res.setHeader("Content-Disposition", "attachment; filename=WeatherReducer.txt");
  res.send(weatherReducerCode);
});

app.get("/download/matrixMapper", (req, res) => {
  res.setHeader("Content-Disposition", "attachment; filename=Matrix_Mapper.txt");
  res.send(matrixMapperCode);
});

app.get("/download/tagMapper", (req, res) => {
  res.setHeader("Content-Disposition", "attachment; filename=TagMapper.txt");
  res.send(tagMapperCode);
});

app.get("/download/wordCountMapper", (req, res) => {
  res.setHeader("Content-Disposition", "attachment; filename=WC_Mapper.txt");
  res.send(wordCountMapperCode);
});


app.get("/copy/weatherReducer", (req, res) => {
  res.json({
    content: weatherReducerCode,
  });
});

app.get("/copy/matrixMapper", (req, res) => {
  res.json({
    content: matrixMapperCode,
  });
});

app.get("/copy/tagMapper", (req, res) => {
  res.json({
    content: tagMapperCode,
  });
});

app.get("/copy/wordCountMapper", (req, res) => {
  res.json({
    content: wordCountMapperCode,
  });
});

// -------------------------------

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
