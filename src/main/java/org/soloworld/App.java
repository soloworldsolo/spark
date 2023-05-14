package org.soloworld;

import io.delta.tables.execution.DeltaConvert;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import io.delta.tables.*;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ParseException {
        SparkSession spark = SparkSession.builder().
                appName("Solo-spark").master("local[1]").getOrCreate();



      DeltaTable.
                convertToDelta(spark, "parquet.`/Users/hokage/Downloads/python-paraquet`");

        spark.stop();
    }
}
