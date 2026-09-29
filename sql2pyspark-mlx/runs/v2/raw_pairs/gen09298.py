from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "enterprise")
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
