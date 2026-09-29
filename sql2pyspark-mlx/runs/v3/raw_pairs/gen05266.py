from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 6]))
    .groupBy("store_id", "product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
    .orderBy(F.col("product_id").desc())
)
