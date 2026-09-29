from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("status").isin(["RETURNED", "COMPLETED", "CANCELLED"]))

orders_stats = (
    valid_orders
    .groupBy(
        F.when(F.col("amount") >= 1390, "high").when(F.col("amount") >= 640, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)

result = orders_stats.orderBy("amount_bucket")
