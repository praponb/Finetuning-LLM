from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("revenue") >= 4500, "high").when(F.col("revenue") >= 500, "mid").otherwise("low").alias("revenue_bucket"),
        "product_id",
    )
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .orderBy("revenue_bucket", F.col("avg_revenue").desc(), "store_id", "product_id")
    .limit(20)
)
