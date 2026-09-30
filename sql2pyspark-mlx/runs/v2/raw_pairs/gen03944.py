from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .filter(F.col("pct_cancelled") < 60)
    .select("status", "max_monthly_fee", "pct_cancelled")
    .orderBy(F.col("max_monthly_fee").desc())
)
