from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([4, 5, 8]))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy("active_monthly_fee", "monthly_fee_range")
)
