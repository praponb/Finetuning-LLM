from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("product_stats.quantity_count"),
        F.col("product_stats.min_unit_price"),
    )
    .orderBy("id")
    .limit(3)
)
