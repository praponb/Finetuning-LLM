from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last(), "id")

ranked = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("temp_c") >= 34.8, "HIGH").otherwise("LOW").alias("temp_c_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("status_bits", 0.95).alias("status_bits_p95"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.sum("humidity").alias("total_humidity"),
        F.stddev_samp("humidity").alias("humidity_stddev_2"),
    )
    .filter(F.col("status_bits_p95") >= 0)
    .orderBy(F.col("duration_min").desc_nulls_first())
)
