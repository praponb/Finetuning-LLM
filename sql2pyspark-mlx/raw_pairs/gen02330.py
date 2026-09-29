from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code").orderBy(F.col("pressure_minus_status_bits").asc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy("humidity_rounded", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status_bits_avg_3_tier").orderBy(F.col("dv_01_avg_humidity").desc())

filtered = (
    df_device_readings
    .filter(F.col("pressure") < 1019)
    .withColumn("humidity_rounded", F.round("humidity", 0))
    .withColumn("pressure_minus_status_bits", (F.col("pressure") - F.col("status_bits")))
)

with_windows = (
    filtered
    .filter(F.col("humidity_rounded").between(44, 67))
    .withColumn("humidity_avg_3", F.avg("humidity").over(w_running))
    .withColumn("pressure_minus_status_bits_percent_rank", F.percent_rank().over(w2))
    .withColumn("status_bits_avg_3", F.avg("status_bits").over(w3))
)

filtered_device_readings = (
    with_windows
    .filter(F.col("temp_c") <= 32)
    .withColumn("temp_c_minus_humidity_avg_3", (F.col("temp_c") - F.col("humidity_avg_3")))
)

prev_events = filtered_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w4))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "humidity_rounded",
        "humidity_avg_3",
        "status_bits_avg_3",
        "temp_c_minus_humidity_avg_3",
        F.sum("is_new_burst").over(w5).alias("island_id"),
    )
)

grouped = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("status_bits_avg_3") >= 12, "PREMIUM").otherwise("STANDARD").alias("status_bits_avg_3_tier"),
        "island_id",
    )
    .agg(
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("humidity"))).alias("dv_01_avg_humidity"),
    )
    .filter(F.col("distinct_device_codes") > 1)
    .withColumn("dv_01_avg_humidity_percent_rank", F.percent_rank().over(w6))
)

result = (
    grouped
    .select(
        "status_bits_avg_3_tier",
        "island_id",
        "distinct_device_codes",
        "dv_01_avg_humidity_percent_rank",
    )
    .orderBy(F.col("status_bits_avg_3_tier").desc())
)
