from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 6]))
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 33, "high").when(F.col("units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("revenue_count") >= 3)
    .orderBy(F.col("product_id").desc())
)
