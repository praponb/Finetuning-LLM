from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_rev"))
    .filter(F.col("total_rev") >= 10000)
)
