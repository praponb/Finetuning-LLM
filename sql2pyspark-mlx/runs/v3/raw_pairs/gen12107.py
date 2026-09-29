from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "PAUSED", "ACTIVE"]) & (F.year("end_date") >= 2025))
)

subscriptions_stats = (
    src
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .select("plan", "monthly_fee_count", "avg_monthly_fee")
    .orderBy("plan")
    .limit(5)
)
