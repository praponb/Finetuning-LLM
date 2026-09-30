from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .filter(F.col("product_stats.unique_products") >= 4)
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.cost"),
        F.col("product_stats.unique_products"),
        F.col("product_stats.revenue_count"),
    )
    .orderBy(F.col("unique_products").desc())
)
