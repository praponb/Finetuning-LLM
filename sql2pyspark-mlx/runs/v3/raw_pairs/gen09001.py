from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 70)
    .select("product_id", "avg_revenue")
)
