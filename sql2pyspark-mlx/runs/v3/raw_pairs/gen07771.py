from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(~(F.col("customer_id").isin([2, 17]) & (F.col("status") != "CANCELLED") & (F.col("customer_id") == 40)))
)

subscriptions_stats = (
    base
    .groupBy("status")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .select("status", "last_end_date", "min_monthly_fee")
    .orderBy("status", F.col("last_end_date").desc())
)
