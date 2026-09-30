from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 1400) & F.col("product_id").isin([2, 3, 5, 10]))
    .groupBy(
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") > 51)
)
