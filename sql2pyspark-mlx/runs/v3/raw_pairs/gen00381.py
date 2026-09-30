from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
    .filter(F.col("revenue_count") < 7)
    .orderBy("revenue_count", "store_id")
    .limit(10)
)
