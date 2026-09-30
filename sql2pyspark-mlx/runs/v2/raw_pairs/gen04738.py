from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "right")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter((F.col("customers.credit_limit") <= 15200) & ~F.col("sessions.country").isin(["DE", "US"]))
    .select(
        F.col("web_events.event_time"),
        F.col("web_events.event_type"),
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("customers.city"),
        F.col("customers.signup_date"),
    )
    .orderBy("event_time")
)
