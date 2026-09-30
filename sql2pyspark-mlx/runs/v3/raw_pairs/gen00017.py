from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 3100) & F.col("store_id").isin([4, 5]))
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 1800)
)
