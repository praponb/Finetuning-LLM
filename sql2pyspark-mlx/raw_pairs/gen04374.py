from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("last3_sum_duration_sec_sum_7d").asc_nulls_last(), "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("duration_sec_sum_7d"),
        F.lag("duration_sec", 2).over(w_part).alias("prev_duration_sec"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") != "tablet")
    .select("src.*", F.col("d.browser"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.sum("duration_sec_sum_7d").over(w3).alias("last3_sum_duration_sec_sum_7d"),
        F.min("duration_sec_sum_7d").over(w3).alias("last3_min_duration_sec_sum_7d"),
        F.count("duration_sec_sum_7d").over(w3).alias("last3_count_duration_sec_sum_7d"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

result = (
    sessionized
    .groupBy(
        F.when(F.col("last3_count_duration_sec_sum_7d") >= 14, "HIGH").otherwise("LOW").alias("last3_count_duration_sec_sum_7d_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max("event_time").alias("last_event_time"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count("*").alias("event_count"),
        F.percentile_approx("last3_min_duration_sec_sum_7d", [0.25, 0.5, 0.75]).alias("last3_min_duration_sec_sum_7d_quartiles"),
    )
    .orderBy(F.col("num_event_types").desc())
)
