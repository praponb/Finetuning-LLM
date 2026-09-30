from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("reading_ts", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") - F.col("temp_c")).alias("humidity_minus_temp_c"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        F.ntile(5).over(w).alias("status_bits_bucket"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_6_prior"),
        F.ntile(4).over(w_grp).alias("status_bits_bucket_2"),
        F.ntile(3).over(w4).alias("humidity_bucket"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w5) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w6))

prev_events = sessions_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w5))

with_gaps_2 = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings_2 = with_gaps_2.withColumn("burst_id_2", F.sum("session_start_flag").over(w6))

enriched_device_readings = (
    sessions_device_readings_2
    .filter(F.col("session_start_flag") < 0)
    .select(
        "*",
        F.avg("status_bits_bucket_2").over(w_recent).alias("last3_avg_status_bits_bucket_2"),
        F.count("status_bits_bucket_2").over(w_recent).alias("last3_count_status_bits_bucket_2"),
        F.min("status_bits_bucket_2").over(w_recent).alias("last3_min_status_bits_bucket_2"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "humidity",
        "status_bits_avg_6_prior",
        "status_bits_bucket_2",
        "humidity_bucket",
        "is_new_session",
        "burst_id",
        "session_start_flag",
        "burst_id_2",
    )
    .orderBy("status_bits_avg_6_prior")
)
