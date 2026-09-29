from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("web_events.event_type").isin(["click", "purchase", "add_to_cart"]))
    .select(
        F.col("web_events.duration_sec"),
        F.col("sessions.browser"),
        F.col("customers.age"),
        F.col("customers.credit_limit"),
    )
    .orderBy("browser")
)
