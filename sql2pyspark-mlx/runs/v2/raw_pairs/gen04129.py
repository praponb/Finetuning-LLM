from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.name").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("c.name"),
        F.col("c.city"),
    )
)
