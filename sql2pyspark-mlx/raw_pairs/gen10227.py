from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.concat("firmware", F.lit(" "), "device_code").alias("firmware_device_code"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.sum("burst_id").alias("total_burst_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.min("temp_c").alias("min_temp_c"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.percentile_approx("temp_c", 0.95).alias("temp_c_p95"),
    )
    .orderBy(F.col("total_burst_id").desc())
)
