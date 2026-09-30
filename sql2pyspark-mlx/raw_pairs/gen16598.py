from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.browser").isin(["Firefox", "Edge"]) & (F.to_date(F.col("sessions.started_at")) >= "2025-07-01"))
    .select(
        F.col("web_events.event_type"),
        F.col("web_events.page"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.col("customers.is_active"),
        F.col("customers.country"),
    )
)
