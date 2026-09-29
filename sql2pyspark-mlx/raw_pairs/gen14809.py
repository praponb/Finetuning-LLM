from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.count("revenue").alias("revenue_count"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.col("min_revenue").desc())
)
