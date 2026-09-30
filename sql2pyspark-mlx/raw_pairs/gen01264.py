from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-05-01", "2025-09-01"))
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)
