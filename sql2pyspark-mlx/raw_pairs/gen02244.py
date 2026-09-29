from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 98, "high").when(F.col("cost") >= 15, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("total_cost") >= 450)
    .select("carrier", "cost_bucket", "total_cost", "pct_delivered")
    .orderBy(F.col("pct_delivered").desc(), "carrier", "cost_bucket")
    .limit(5)
)
