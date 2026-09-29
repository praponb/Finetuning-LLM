from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
    .filter(F.col("avg_unit_price") >= 310)
    .orderBy(F.col("revenue").desc())
)
