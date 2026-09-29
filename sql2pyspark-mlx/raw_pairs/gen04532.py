from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("session_id", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("island_id_sum_30m").desc_nulls_first(), "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("dedup_rank").desc(), "id")

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

with_gaps = (
    sessionized
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

enriched = (
    sessionized_device_readings
    .filter(F.to_date("reading_ts") < "2024-06-25")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "session_id",
        F.sum("island_id").over(w_hist).alias("island_id_sum_30m"),
        F.avg("is_new_burst").over(w4).alias("device_code_avg_is_new_burst"),
        F.lead("is_new_burst", 1).over(w5).alias("next_is_new_burst"),
        F.avg("session_id").over(w_hist).alias("session_id_avg_30m"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_prev = ranked.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged_2 = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings_2 = flagged_2.withColumn("burst_id", F.sum("is_new_session").over(w2))

latest = (
    sessionized_device_readings_2
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

daily = (
    latest
    .groupBy("session_id")
    .agg(
        F.min("device_code_avg_is_new_burst").alias("min_device_code_avg_is_new_burst"),
        F.percentile_approx("island_id_sum_30m", 0.95).alias("island_id_sum_30m_p95"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.max("is_new_session").alias("max_is_new_session"),
    )
)

result = (
    daily
    .select(
        "session_id",
        "min_device_code_avg_is_new_burst",
        "distinct_device_codes",
        "max_is_new_session",
    )
)
