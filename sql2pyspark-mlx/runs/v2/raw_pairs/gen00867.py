from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("c.country").alias("customer_country"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("event_time").desc(), F.col("customer_country").desc())
)
