from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "high").when(F.col("discount") >= 0.07, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
)
