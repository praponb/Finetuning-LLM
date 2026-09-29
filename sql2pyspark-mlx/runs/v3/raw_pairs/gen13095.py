from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
)
