from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.event_type") == "add_to_cart")
    .select(
        F.col("we.event_type"),
        F.col("s.device"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.date_trunc("YEAR", F.col("s.started_at")).alias("started_at_period"),
    )
    .orderBy("device")
)
