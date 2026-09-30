from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("order_items_by_order.total_quantity"),
        F.col("order_items_by_order.quantity_count"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("total_quantity").desc())
)
