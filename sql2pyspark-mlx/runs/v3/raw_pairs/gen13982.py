from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("product_id").isin([4, 9, 15, 19]))
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.store_id"), F.col("t.avg_quantity"), F.col("t.num_order_items"))
    .orderBy("num_order_items")
)
