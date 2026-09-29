from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.when(F.col("cost") >= 800, "high").when(F.col("cost") >= 210, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.avg("price").alias("avg_price"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .filter(F.col("avg_cost") >= 76)
    .orderBy(F.col("avg_cost").desc(), "avg_price")
)
