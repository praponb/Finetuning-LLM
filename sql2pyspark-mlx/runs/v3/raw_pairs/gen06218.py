from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.revenue") > 71)
    .select(
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("t.revenue"),
        F.col("t.row_count"),
    )
    .orderBy(F.col("discount").desc(), F.col("revenue").desc())
)
