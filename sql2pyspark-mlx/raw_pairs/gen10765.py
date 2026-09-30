from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 83, "high").when(F.col("cost") >= 26, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 41)
    .orderBy(F.col("cost_bucket").desc())
)
