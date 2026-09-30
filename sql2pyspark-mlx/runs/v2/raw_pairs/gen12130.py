from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("we.event_type") == "purchase")
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("c.country"),
        F.col("c.is_active"),
    )
)
