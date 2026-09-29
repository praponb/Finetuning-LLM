from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits_num", "id")
w2 = Window.orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("session_no", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("next_temp_c_per_humidity_2").asc_nulls_first())
w9 = Window.partitionBy("device_code").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_num", F.col("status_bits").cast("double"))
)

cleaned = (
    filtered
    .filter(F.col("reading_ts") > F.lit("2025-09-01 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.col("temp_c") / F.col("humidity")), 2).alias("temp_c_per_humidity"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("temp_c_per_humidity", 2, 0).over(w).alias("next_temp_c_per_humidity"),
        F.lead("temp_c_per_humidity", 2).over(w2).alias("next_temp_c_per_humidity_2"),
        F.first("pressure", ignorenulls=True).over(w3).alias("first_known_pressure"),
        F.min("status_bits_num").over(w4).alias("device_code_min_status_bits_num"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w5) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

with_windows = (
    sessionized
    .withColumn("is_new_session_avg_2", F.avg("is_new_session").over(w7))
    .withColumn("next_temp_c_per_humidity_2_rank", F.rank().over(w_grp))
    .withColumn("running_next_temp_c_per_humidity_2", F.sum("next_temp_c_per_humidity_2").over(w9))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w10))
)

with_gaps = (
    with_windows
    .withColumn("is_new_session_2", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w5) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("is_new_session_2").over(w6))

grouped = (
    sessions_device_readings
    .rollup(
        "session_no",
        F.when(F.col("first_known_temp_c") >= 35.5, "LARGE").otherwise("SMALL").alias("first_known_temp_c_tier"),
    )
    .agg(
        F.min("reading_ts").alias("started_at"),
        F.max(F.when(F.col("device_code") == "DV-04", F.col("session_no"))).alias("dv_04_max_session_no"),
        F.min("reading_ts").alias("started_at_2"),
        F.min("reading_ts").alias("first_seen"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.max("reading_ts").alias("last_seen"),
    )
)

result = grouped.select("session_no", "started_at", "last_seen")
