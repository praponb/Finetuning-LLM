from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("order_id") == 34)
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .filter(F.col("oibo.unique_orders") >= 9)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("oibo.unique_orders"),
        F.col("oibo.quantity_count"),
    )
)
