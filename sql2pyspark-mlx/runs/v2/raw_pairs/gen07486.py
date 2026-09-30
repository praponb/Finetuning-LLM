from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("status").isin(["CANCELLED", "PAUSED"]))

summary = (
    filtered_subscriptions
    .groupBy("status", "plan")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = summary.orderBy("min_monthly_fee")
