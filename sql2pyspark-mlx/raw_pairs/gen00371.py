from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2600)
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
    .filter(F.col("revenue_count") > 9)
    .orderBy(F.col("store_id").desc())
)
