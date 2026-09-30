from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 90, "high").when(F.col("cost") >= 18, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 40)
    .select("carrier", "cost_bucket", "min_cost", "avg_cost")
    .orderBy("cost_bucket", F.col("carrier").desc())
    .limit(20)
)
