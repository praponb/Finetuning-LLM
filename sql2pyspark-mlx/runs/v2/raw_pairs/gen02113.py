from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.duration_sec_range") > 41)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("t.duration_sec_range"),
        F.col("t.latest_event_time"),
    )
)
