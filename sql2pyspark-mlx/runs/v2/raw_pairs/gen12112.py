from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", "plan", "status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 170)
    .drop("avg_monthly_fee")
)
