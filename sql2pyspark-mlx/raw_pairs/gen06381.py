from pyspark.sql import functions as F

base = df_subscriptions.filter(F.col("start_date") < "2024-08-25")

agg = (
    base
    .groupBy("status", "plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("subscription_count"),
    )
)

result = (
    agg
    .filter(F.col("subscription_count") > 6)
    .orderBy(F.col("subscription_count").desc())
)
