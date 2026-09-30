from pyspark.sql import functions as F

base = df_shipments.filter(F.col("order_id").isNotNull())

summary = (
    base
    .groupBy(
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 20, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = summary.select("cost_bucket", "pct_delivered", "avg_cost")
