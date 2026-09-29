from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)
