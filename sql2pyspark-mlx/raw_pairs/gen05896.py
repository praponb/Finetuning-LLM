from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("ps.total_count"),
        F.col("ps.store_sale_count"),
    )
    .orderBy("id")
)
