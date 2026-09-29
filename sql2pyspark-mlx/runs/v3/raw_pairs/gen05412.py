from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") > 13400)
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.when(F.col("we.event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
    .orderBy(F.col("add_to_cart_count").desc())
)
