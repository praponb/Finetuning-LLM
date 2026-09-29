from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium", "basic"]) & F.col("plan").isin(["standard", "basic"]) & (F.col("status") != "ACTIVE"))
)

summary = (
    valid_subscriptions
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = (
    summary
    .filter(F.col("monthly_fee_range") >= 50)
    .select("plan", "monthly_fee_range")
    .orderBy(F.col("monthly_fee_range").desc(), "plan")
    .limit(10)
)
