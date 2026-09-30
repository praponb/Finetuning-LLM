from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.shipping_fee"), F.col("O.stddev_quantity"))
)
