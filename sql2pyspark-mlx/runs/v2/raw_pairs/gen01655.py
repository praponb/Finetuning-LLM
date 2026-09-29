from pyspark.sql import functions as F

base = df_store_sales.filter((F.col("units") > 28) & F.col("product_id").isin([3, 4]))

store_sales_stats = (
    base
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    store_sales_stats
    .select("store_id", "avg_revenue")
    .orderBy(F.col("avg_revenue").desc(), F.col("store_id").desc())
)
