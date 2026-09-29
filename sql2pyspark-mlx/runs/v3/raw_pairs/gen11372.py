from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2025-04-27") & (F.col("revenue").between(1500, 2000) | (F.col("product_id") == 2)))
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)
