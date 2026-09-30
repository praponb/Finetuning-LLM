from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("status").isin(["PAUSED", "EXPIRED"]))

summary = (
    valid_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    summary
    .orderBy(F.col("avg_monthly_fee").desc(), "status")
    .limit(10)
)
