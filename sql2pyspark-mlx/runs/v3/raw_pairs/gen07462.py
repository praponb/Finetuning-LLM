from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "left")
    .filter(F.col("product_summary.total_revenue") >= 13000)
    .select(
        F.col("products.brand"),
        F.col("products.supplier_id"),
        F.col("product_summary.total_revenue"),
        F.col("product_summary.revenue_range"),
    )
    .orderBy(F.col("revenue_range").desc())
)
