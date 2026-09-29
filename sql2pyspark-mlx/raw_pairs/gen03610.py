from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 95, "high").when(F.col("cost") >= 38, "mid").otherwise("low").alias("cost_bucket"),
        "order_id",
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("cost_bucket", "order_id")
)
