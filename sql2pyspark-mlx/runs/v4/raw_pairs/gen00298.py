from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
    )
    .orderBy("signup_date")
)
