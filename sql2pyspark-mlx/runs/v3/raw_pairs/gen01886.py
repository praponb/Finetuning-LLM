from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.customer_id").isin([25, 31]))
    .select(
        F.col("sessions.country"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
        F.col("web_events.event_type"),
    )
)
