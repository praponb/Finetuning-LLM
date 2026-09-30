from pyspark.sql import functions as F

base = df_subscriptions.filter(F.col("status").isin(["CANCELLED", "PAUSED"]))

subscriptions_stats = (
    base
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .select("plan", "end_quarter", "min_monthly_fee")
    .orderBy(F.col("end_quarter").desc(), F.col("plan").desc())
    .limit(10)
)
