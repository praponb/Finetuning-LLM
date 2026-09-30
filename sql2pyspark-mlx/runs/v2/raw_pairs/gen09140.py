from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("amount").between(1320, 1350) & ((F.col("status") == "SHIPPED") | (F.col("order_date") <= "2025-02-01")))
)

orders_stats = (
    recent_orders
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
    )
)

result = (
    orders_stats
    .select("customer_id", "web_discount")
    .orderBy("customer_id", "web_discount")
)
