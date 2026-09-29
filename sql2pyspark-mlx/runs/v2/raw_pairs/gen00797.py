from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 4) & F.col("sale_date").between("2024-06-16", "2025-04-30"))
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("*").alias("store_sale_count"),
    )
    .filter(F.col("store_sale_count") < 3)
)
