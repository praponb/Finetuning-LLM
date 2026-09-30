from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("we.page").isin(["/search", "/product"]))
    .groupBy(F.col("s.device"))
    .agg(
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"),
    )
    .orderBy("cart_count", "device")
    .limit(5)
)
