from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
