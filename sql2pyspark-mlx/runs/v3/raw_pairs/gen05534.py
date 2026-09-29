from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-01-13", "2025-01-24"))
    .groupBy("product_id")
    .agg(F.count("revenue").alias("revenue_count"))
    .filter(F.col("revenue_count") >= 10)
    .select("product_id", "revenue_count")
)
