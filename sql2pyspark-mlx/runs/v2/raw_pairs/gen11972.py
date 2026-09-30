from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
    .orderBy(F.col("store_id").desc())
    .limit(50)
)
