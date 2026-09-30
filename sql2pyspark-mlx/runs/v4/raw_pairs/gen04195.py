from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") < 0.18)
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
    .orderBy("revenue")
)
