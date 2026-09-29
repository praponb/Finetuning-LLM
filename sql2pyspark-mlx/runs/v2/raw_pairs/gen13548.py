from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .filter((F.col("sessions.customer_id") == 27) | (F.col("sessions.browser") == "Safari"))
    .select(
        F.col("web_events.page"),
        F.col("web_events.session_id"),
        F.col("web_events.event_time"),
        F.col("sessions.device"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
    )
    .orderBy("event_time")
)
