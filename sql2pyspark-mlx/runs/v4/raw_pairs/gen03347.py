from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 22)
    .groupBy(
        F.when(F.col("units") >= 36, "high").when(F.col("units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("min_revenue") < 3500)
    .select("units_bucket", "min_revenue", "stddev_revenue")
    .orderBy("min_revenue")
)
