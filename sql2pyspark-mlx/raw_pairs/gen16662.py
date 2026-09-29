from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 90, "high").when(F.col("cost") >= 17, "mid").otherwise("low").alias("cost_bucket"),
        "status",
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy(F.col("status").desc(), "cost_bucket")
    .limit(20)
)
