from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("monthly_fee") <= 130)

summary = (
    src
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    summary
    .filter(F.col("enterprise_monthly_fee") < 49)
    .select("status", "enterprise_monthly_fee")
    .orderBy(F.col("enterprise_monthly_fee").desc())
)
