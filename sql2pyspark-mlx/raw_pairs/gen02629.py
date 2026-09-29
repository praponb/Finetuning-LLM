from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("temp_c") >= 26)
    .select(
        "*",
        F.concat_ws("-", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_device_code_key",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy("session_id")
    .agg(
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
        F.avg("humidity").alias("avg_humidity"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .orderBy("status_bits_stddev")
)
