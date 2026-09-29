from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.started_at") >= "2023-07-07")
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "page_view", F.col("we.duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
    .orderBy(F.col("web_event_count").desc(), "device")
    .limit(10)
)
