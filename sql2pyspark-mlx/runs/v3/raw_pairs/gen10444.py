from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-02-01")
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.started_at"), F.col("t.duration_sec_range"))
    .orderBy("id", F.col("started_at").desc())
    .limit(50)
)
