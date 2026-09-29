from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.device") == "tablet")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.duration_sec"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("customers.country").alias("customer_country"),
    )
    .orderBy("event_time")
)
