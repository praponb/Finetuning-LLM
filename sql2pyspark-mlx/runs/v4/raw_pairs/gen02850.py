from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("quantity") == 4)
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .filter(F.col("oibo.unique_products") > 9)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("oibo.unique_products"),
    )
    .orderBy("shipping_fee")
)
