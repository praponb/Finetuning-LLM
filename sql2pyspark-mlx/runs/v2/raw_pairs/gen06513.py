from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_quantity") >= 7.2)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("t.avg_quantity"),
    )
)
