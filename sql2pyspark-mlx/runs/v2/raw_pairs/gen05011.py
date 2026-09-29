from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("order_item_count") >= 4)
)
