from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("dedup_rank", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_web_events = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "event_time", "duration_sec")
)

gaps = (
    ranked_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w4).alias("island_id"),
    )
)

with_gaps = (
    sessions_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events_2 = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w4))

with_dims = (
    sessions_web_events_2
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .filter(F.hour(F.col("p.started_at")).between(9, 16))
    .select(
        "src.id",
        "src.session_id",
        "src.event_time",
        "src.duration_sec",
        F.col("p.browser"),
    )
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("sum_web_event_duration_sec").asc_nulls_last())
)
