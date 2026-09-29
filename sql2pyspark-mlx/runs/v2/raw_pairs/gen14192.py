from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("order_date") <= "2024-02-01")

orders_stats = (
    filtered_orders
    .groupBy("channel")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
)

result = (
    orders_stats
    .filter(F.col("stddev_shipping_fee") < 47)
    .select("channel", "stddev_shipping_fee")
)
