from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("order_date") < "2025-02-01")

orders_stats = (
    valid_orders
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
)

result = orders_stats.orderBy(F.col("avg_shipping_fee").desc())
