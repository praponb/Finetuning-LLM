from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .groupBy(
        F.when(F.col("revenue") >= 3500, "high").when(F.col("revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") > 44)
    .orderBy("avg_units")
)
