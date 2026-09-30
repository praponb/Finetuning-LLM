from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-07-01")
    .groupBy("store_id", "product_id", F.month("sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("*").alias("total_count"),
    )
    .orderBy("unique_products")
)
