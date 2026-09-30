from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .filter(F.col("sessions.country").isin(["UK", "US", "DE"]) & (F.col("web_events.duration_sec") != 380))
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
        F.col("web_events.event_type"),
    )
    .orderBy(F.col("started_at").desc(), "customer_id")
)
