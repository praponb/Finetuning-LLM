from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "page_view", "purchase"]))
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.avg_duration_sec") >= 190)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("ss.last_event_time"),
        F.col("ss.avg_duration_sec"),
    )
)
