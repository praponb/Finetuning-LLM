from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") < "2025-01-01") | (F.col("end_date") <= "2024-09-01"))
)

agg = (
    valid_subscriptions
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    agg
    .filter(F.col("stddev_monthly_fee") > 42)
    .select("plan", "end_quarter", "stddev_monthly_fee")
    .orderBy("end_quarter", F.col("stddev_monthly_fee").desc())
)
