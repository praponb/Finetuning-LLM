from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.status"),
        F.col("order_items_by_order.revenue"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
