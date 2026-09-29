from pyspark.sql import functions as F

valid_orders = df_orders.filter((F.col("order_date") <= "2023-12-06") & (F.col("store_id") == 20))

orders_stats = (
    valid_orders
    .groupBy("status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    orders_stats
    .select("status", "stddev_amount")
    .orderBy(F.col("stddev_amount").desc(), F.col("status").desc())
)
