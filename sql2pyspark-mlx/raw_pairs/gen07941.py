from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("P.revenue_count"),
        F.col("P.min_sale_date"),
    )
    .orderBy(F.col("min_sale_date").desc())
)
