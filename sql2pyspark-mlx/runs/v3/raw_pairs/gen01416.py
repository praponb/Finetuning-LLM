from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("web_events.duration_sec").between(190, 331))
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("web_events.event_time"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
    )
    .orderBy("started_at", "id")
    .limit(10)
)
