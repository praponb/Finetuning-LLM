from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("*").alias("num_subscriptions"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .orderBy("pct_premium")
)
