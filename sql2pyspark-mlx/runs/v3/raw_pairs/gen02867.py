from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .filter(F.col("order_stats.stddev_unit_price") >= 61)
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("order_stats.stddev_unit_price"),
    )
)
