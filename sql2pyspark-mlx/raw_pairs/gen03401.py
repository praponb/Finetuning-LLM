from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2024-12-01")
    .groupBy("product_id", "store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy(F.col("total_revenue").desc())
)
