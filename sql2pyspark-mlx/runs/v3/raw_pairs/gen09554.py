from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_time") >= "2024-12-01")
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") >= 260)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("t.min_duration_sec"))
    .orderBy(F.col("started_at").desc())
)
