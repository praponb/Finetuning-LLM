from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").isNull())
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") >= 65)
)
