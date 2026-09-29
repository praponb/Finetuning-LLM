from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3000, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
        "store_id",
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("avg_revenue") >= 45)
    .orderBy(F.col("revenue_bucket").desc(), F.col("store_id").desc())
    .limit(5)
)
