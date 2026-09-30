from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "CANCELLED"]) & (F.col("status") != "ACTIVE"))
)

agg = (
    base
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("premium_monthly_fee") > 70)
    .select("end_month", "premium_monthly_fee")
    .orderBy(F.col("premium_monthly_fee").desc())
)
