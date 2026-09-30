from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy(F.col("total_monthly_fee").desc())
)
