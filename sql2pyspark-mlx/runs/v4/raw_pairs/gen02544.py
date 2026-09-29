from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
    .orderBy("enterprise_monthly_fee")
)
