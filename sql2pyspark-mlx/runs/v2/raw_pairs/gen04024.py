from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.device") != "desktop")
    .select(
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.col("web_events.event_type"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
    )
    .orderBy("customer_id")
)
