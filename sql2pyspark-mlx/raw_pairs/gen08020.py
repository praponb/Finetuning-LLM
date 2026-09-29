from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("we.event_time") < "2025-03-01")
    .groupBy(F.col("s.browser"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
)
