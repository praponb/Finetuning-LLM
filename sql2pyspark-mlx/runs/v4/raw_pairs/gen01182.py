from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3400, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_units") < 220)
    .orderBy(F.col("total_units").desc())
)
