from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1800)
    .agg(F.min("revenue").alias("min_revenue"))
    .orderBy("min_revenue")
)
