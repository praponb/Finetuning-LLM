from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
    )
    .orderBy("city", "cart_count")
)
