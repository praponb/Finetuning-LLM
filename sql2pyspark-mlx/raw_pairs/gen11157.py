from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.to_date(F.col("sessions.started_at")) < "2025-09-01")
    .select(
        F.col("web_events.session_id"),
        F.col("web_events.event_type"),
        F.col("web_events.event_time"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
    )
    .orderBy(F.col("event_type").desc(), "device")
)
