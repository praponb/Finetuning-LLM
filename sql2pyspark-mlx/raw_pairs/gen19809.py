from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity") != 5)
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("product_stats.unit_price_count"),
        F.col("product_stats.avg_unit_price"),
    )
)
