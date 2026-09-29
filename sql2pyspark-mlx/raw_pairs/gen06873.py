from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.unit_price") <= 340)
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
)
