from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "left")
    .filter(F.col("oibo.avg_quantity") > 3.3)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("oibo.avg_quantity"),
    )
)
