from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity").between(3, 6))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.quantity_count"),
        F.col("t.avg_unit_price"),
    )
    .orderBy(F.col("quantity_count").desc())
)
