from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "enterprise"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("expired_monthly_fee") > 24)
    .orderBy("expired_monthly_fee")
)
