from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") >= 28)
    .orderBy("customer_id")
    .limit(5)
)
