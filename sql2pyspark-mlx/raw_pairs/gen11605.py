from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.partitionBy("event_type").orderBy("duration_sec_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("is_new_session").desc(), "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w3))
    .withColumn("is_new_session_max_90d", F.max("is_new_session").over(w4))
)

scored = (
    enriched
    .select(
        "id",
        "session_id",
        "burst_id",
        "duration_sec_percent_rank",
        "is_new_session_max_90d",
        F.sum("duration_sec").over(w_recent).alias("running_sum_duration_sec"),
        F.avg("duration_sec").over(w_recent).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w_recent).alias("running_min_duration_sec"),
        F.ntile(4).over(w6).alias("is_new_session_bucket"),
    )
)

valid = (
    scored
    .filter(F.col("duration_sec_percent_rank") >= 0.24)
    .select(
        "*",
        F.round("burst_id", 1).alias("burst_id_rounded"),
        F.round((F.col("is_new_session_max_90d") * 0.9), 2).alias("is_new_session_max_90d_scaled"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.browser").alias("browser"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.running_sum_duration_sec")).alias("min_running_sum_duration_sec"),
        F.round(F.avg(F.col("src.is_new_session_bucket")), 2).alias("avg_is_new_session_bucket"),
    )
)
