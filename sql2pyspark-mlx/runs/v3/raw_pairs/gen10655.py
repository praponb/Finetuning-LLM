from pyspark.sql import functions as F

recent_subscriptions = df_subscriptions.filter(F.col("customer_id").isin([1, 10, 39]))

summary = (
    recent_subscriptions
    .groupBy("status", "plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    summary
    .filter(F.col("min_monthly_fee") < 210)
    .select("status", "plan", "min_monthly_fee")
    .orderBy("plan", F.col("status").desc())
)
