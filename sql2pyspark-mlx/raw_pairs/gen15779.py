from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

latest = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "reading_ts", "temp_c", "humidity", "status_bits", "firmware")
)

result = (
    latest
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("status_bits", 0.9).alias("status_bits_p90"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
    )
    .orderBy("humidity_stddev")
)
