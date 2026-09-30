from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]) & (F.col("monthly_fee") >= 180))
)

plan_summary = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    plan_summary
    .select("plan", "min_monthly_fee")
    .orderBy("plan")
    .limit(10)
)
