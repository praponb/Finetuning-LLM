from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-06-26", "2025-09-01"))
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") < 2)
)
