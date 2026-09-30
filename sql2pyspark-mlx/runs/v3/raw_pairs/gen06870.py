from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & F.month("delivered_date").isin([7, 9, 10]))
)

shipments_stats = (
    base
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 81, "high").when(F.col("cost") >= 37, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    shipments_stats
    .select("carrier", "cost_bucket", "total_cost", "avg_cost")
    .orderBy("total_cost", "carrier", "cost_bucket")
    .limit(1)
)
