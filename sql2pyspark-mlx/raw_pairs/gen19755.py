from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 74)
    .select("product_id", "avg_revenue")
)
