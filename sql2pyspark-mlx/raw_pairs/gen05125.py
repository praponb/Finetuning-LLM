from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.min_unit_price") >= 280)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("t.min_unit_price"),
        F.col("t.unit_price_count"),
    )
    .orderBy(F.col("unit_price_count").desc())
)
