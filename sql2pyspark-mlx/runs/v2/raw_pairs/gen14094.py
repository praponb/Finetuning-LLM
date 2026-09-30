from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 2)
    .orderBy(F.col("store_id").desc(), F.col("total_count").desc())
)
