from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 41, "high").when(F.col("units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.count("revenue").alias("revenue_count"))
    .filter(F.col("revenue_count") < 3)
)
