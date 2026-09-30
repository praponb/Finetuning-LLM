from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") >= 2025) & F.col("revenue").isNotNull())
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.sum("revenue").alias("total_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("total_revenue") >= 15000)
)
