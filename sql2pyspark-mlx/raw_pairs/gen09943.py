from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("monthly_fee").between(100, 190))

agg = (
    recent_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = (
    agg
    .filter(F.col("pct_cancelled") >= 45)
    .orderBy(F.col("total_monthly_fee").desc())
)
