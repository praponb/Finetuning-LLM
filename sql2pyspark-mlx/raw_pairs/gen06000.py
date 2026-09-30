from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("pressure").between(993, 995))
    .withColumn("status_bits_plus_temp_c", (F.col("status_bits") + F.col("temp_c")))
    .withColumn("temp_c_plus_status_bits", (F.col("temp_c") + F.col("status_bits")))
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("temp_c").over(w).alias("prev_temp_c"),
        F.lag("humidity").over(w).alias("prev_humidity"),
        F.lead("humidity").over(w).alias("next_humidity"),
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w2).alias("status_bits_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("prev_temp_c").over(w3).alias("last4_avg_prev_temp_c"),
        F.max("prev_temp_c").over(w3).alias("last4_max_prev_temp_c"),
        F.count("prev_temp_c").over(w3).alias("last4_count_prev_temp_c"),
        F.min("prev_temp_c").over(w3).alias("last4_min_prev_temp_c"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w4).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w5))

stats = (
    islands
    .groupBy(
        F.when(F.col("status_bits_plus_temp_c") >= 75, "PREMIUM").otherwise("STANDARD").alias("status_bits_plus_temp_c_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.min("reading_ts").alias("started_at"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
)

result = (
    stats
    .select(
        "status_bits_plus_temp_c_tier",
        "activity_day",
        "device_code_list",
        "dv_01_events",
        "started_at",
        "status_bits_bit_or",
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
