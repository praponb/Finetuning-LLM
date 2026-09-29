from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("unit_price") < 370)
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.col("order_items_by_order.min_unit_price"),
        F.col("order_items_by_order.stddev_unit_price"),
    )
)
