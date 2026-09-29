from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("t.revenue_count"),
        F.col("t.total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc())
)
