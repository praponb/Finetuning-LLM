from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .groupBy("status", "plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") < 100)
    .orderBy("status", "plan")
    .limit(10)
)
