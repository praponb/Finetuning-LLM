from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("revenue").between(1900, 3700))

revenue_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("revenue") >= 3700, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    revenue_bucket_summary
    .orderBy("revenue_bucket")
    .limit(5)
)
