from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc(), "id", "status")
    .limit(5)
)
