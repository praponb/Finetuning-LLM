from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.max_duration_sec") >= 390)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("ss.last_event_time"),
        F.col("ss.max_duration_sec"),
    )
    .orderBy("last_event_time")
)
