from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_duration_sec") >= 1700)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("t.latest_event_time"),
        F.col("t.total_duration_sec"),
    )
    .orderBy(F.col("id").desc(), "device")
    .limit(20)
)
