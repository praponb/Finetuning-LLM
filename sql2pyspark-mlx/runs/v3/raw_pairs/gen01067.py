from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .filter(F.col("oibo.avg_quantity") >= 7.7)
    .select(
        F.col("o.order_date"),
        F.col("oibo.unique_orders"),
        F.col("oibo.avg_quantity"),
    )
    .orderBy(F.col("unique_orders").desc())
)
