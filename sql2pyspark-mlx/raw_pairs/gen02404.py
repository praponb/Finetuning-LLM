from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue") >= 1800)

summary = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    summary
    .filter(F.col("total_revenue") > 15000)
    .select("store_id", "total_revenue")
    .orderBy(F.col("store_id").desc())
    .limit(20)
)
