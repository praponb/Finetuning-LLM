from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("pressure_rounded", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy(F.col("session_start_flag_avg_3").desc_nulls_first())
w6 = Window.partitionBy("device_code").orderBy("rank_in_group", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.round("pressure", 2).alias("pressure_rounded"),
        F.ceil("pressure").alias("pressure_whole"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

scored = (
    islands
    .select(
        "*",
        F.avg("session_start_flag").over(w3).alias("session_start_flag_avg_3"),
        F.round((F.col("humidity") / F.sum("humidity").over(w4)), 4).alias("humidity_share"),
        F.count("session_start_flag").over(w4).alias("device_code_count_session_start_flag"),
    )
)

filtered_device_readings = (
    scored
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 16:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "pressure_rounded",
        "pressure_whole",
        "session_start_flag",
        "island_id",
        "session_start_flag_avg_3",
        "humidity_share",
        F.floor("humidity").alias("humidity_whole"),
        F.round((F.col("pressure_whole") * 1.1), 2).alias("adjusted_pressure_whole"),
    )
)

with_gaps_2 = (
    filtered_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands_device_readings = with_gaps_2.withColumn("session_no", F.sum("is_new_session").over(w2))

ranked = (
    islands_device_readings
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("pressure_rounded_avg_2", F.avg("pressure_rounded").over(w6))
    .withColumn("device_code_stddev_samp_island_id", F.stddev_samp("island_id").over(w4))
)
