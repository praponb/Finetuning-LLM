from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 120)
    .select("plan", "active_count", "avg_monthly_fee")
)
