from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") >= 74)
    .orderBy("enterprise_monthly_fee", "status")
)
