from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("revenue") <= 1600)

revenue_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("revenue") >= 4500, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    revenue_bucket_summary
    .filter(F.col("stddev_units") < 65)
    .select("revenue_bucket", "stddev_units", "max_revenue")
    .orderBy(F.col("stddev_units").desc(), F.col("max_revenue").desc())
)
