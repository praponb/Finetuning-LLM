from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 3, 4]) & F.col("sale_date").between("2023-03-30", "2024-08-01"))
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy(F.col("product_id").desc())
)
