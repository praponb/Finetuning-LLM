from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("amount") < 420)

summary = (
    filtered_orders
    .groupBy(
        "status",
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 8, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.count("*").alias("order_count"),
    )
)

result = (
    summary
    .filter(F.col("order_count") < 5)
    .select("status", "avg_discount", "order_count")
)
