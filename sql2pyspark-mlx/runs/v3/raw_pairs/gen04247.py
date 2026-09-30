from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 3400)
    .select("store_id", "avg_revenue")
    .orderBy(F.col("store_id").desc())
)
