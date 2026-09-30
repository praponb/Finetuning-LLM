from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.avg_unit_price") >= 63)
    .select(
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("t.avg_unit_price"),
        F.col("t.max_quantity"),
    )
    .orderBy("status")
)
