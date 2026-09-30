from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") & ~F.col("plan").isin(["premium", "basic"]))
)

plan_summary = (
    valid_subscriptions
    .groupBy("plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    plan_summary
    .filter(F.col("min_monthly_fee") >= 120)
    .select("plan", "min_monthly_fee")
    .orderBy("plan")
)
