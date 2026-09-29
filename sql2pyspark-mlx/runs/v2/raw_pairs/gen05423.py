from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3500, "high").when(F.col("revenue") >= 600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
)
