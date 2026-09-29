from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.max_revenue") > 1900)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.store_sale_count"),
        F.col("ps.max_revenue"),
    )
    .orderBy(F.col("store_sale_count").desc())
)
