from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
    .orderBy(F.col("status").desc())
)
