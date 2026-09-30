from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
