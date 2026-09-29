from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isin([13, 15, 34, 40]))
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "left")
    .filter(F.col("order_items_by_order.unique_products") > 9)
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
        F.col("order_items_by_order.unique_products"),
    )
    .orderBy(F.col("id").desc())
)
