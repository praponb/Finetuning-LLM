from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("event_time").alias("last_event_time"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
        F.col("t.last_event_time"),
    )
)
