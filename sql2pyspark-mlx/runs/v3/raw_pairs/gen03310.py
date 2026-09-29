from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") > 61)
    .select("status", "avg_monthly_fee", "monthly_fee_range")
    .orderBy(F.col("monthly_fee_range").desc())
)
