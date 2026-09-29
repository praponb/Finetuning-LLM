from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.min_revenue"),
        F.col("t.unique_stores"),
    )
    .orderBy("id")
)
