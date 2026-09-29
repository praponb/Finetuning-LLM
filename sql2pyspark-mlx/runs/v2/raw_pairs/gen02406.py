from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("product_id").isin([2, 9, 10]) & (F.col("store_id") == 6)) | (F.col("sale_date") > "2024-03-01"))
)

summary = (
    base
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    summary
    .filter(F.col("avg_revenue") < 22)
    .orderBy(F.col("store_id").desc(), F.col("avg_revenue").desc())
)
