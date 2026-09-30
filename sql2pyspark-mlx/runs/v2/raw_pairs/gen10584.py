from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("status") != "CANCELLED")

summary = (
    valid_subscriptions
    .groupBy("plan", F.quarter("start_date").alias("start_quarter"))
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
)

result = (
    summary
    .select("plan", "start_quarter")
    .orderBy(F.col("plan").desc(), "start_quarter")
    .limit(1)
)
