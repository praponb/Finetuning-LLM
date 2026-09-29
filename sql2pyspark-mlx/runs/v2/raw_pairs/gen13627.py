from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") <= 17)
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
    )
    .orderBy("status")
    .limit(3)
)
