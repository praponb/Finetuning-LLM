from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 100) & F.col("plan").isin(["enterprise", "basic", "standard"]))
)

agg = (
    valid_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)

result = (
    agg
    .filter(F.col("avg_monthly_fee") >= 180)
    .select("start_month", "avg_monthly_fee")
    .orderBy(F.col("start_month").desc())
    .limit(1)
)
