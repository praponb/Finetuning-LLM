from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)
