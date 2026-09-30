from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") <= 7)
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.avg_quantity") >= 68)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("os.avg_quantity"),
    )
    .orderBy("shipping_fee")
)
