from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.length(F.col("c.email")) > 10)
    .groupBy(F.col("c.city"))
    .agg(F.count(F.when(F.col("we.page") == "/cart", 1)).alias("cart_count"))
)
