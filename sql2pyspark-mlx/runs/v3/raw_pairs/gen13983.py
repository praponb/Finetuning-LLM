from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3200)
    .groupBy(
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy("units_bucket", F.col("avg_units").desc())
)
