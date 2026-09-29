from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.city"),
    )
    .orderBy("customer_id", "email")
)
