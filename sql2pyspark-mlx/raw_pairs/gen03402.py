from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") >= 210)
    .drop("max_monthly_fee")
)
