from pyspark.sql import functions as F

result = (
    df_store_sales
    .cube(
        "product_id",
        F.when(F.col("revenue") >= 3100, "high").when(F.col("revenue") >= 700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .orderBy(F.col("avg_revenue").desc())
)
