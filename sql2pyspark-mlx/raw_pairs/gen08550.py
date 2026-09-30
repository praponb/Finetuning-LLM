from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.session_id") == 7)
    .groupBy(F.col("s.device"))
    .agg(
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("we.event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .orderBy(F.col("device").desc(), F.col("avg_duration_sec").desc())
)
