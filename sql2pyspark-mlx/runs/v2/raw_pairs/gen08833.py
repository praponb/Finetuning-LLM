from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") > 71)
    .select("plan", "unique_plans", "stddev_monthly_fee")
)
