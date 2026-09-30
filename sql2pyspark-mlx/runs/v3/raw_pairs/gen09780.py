from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") > 22)
    .select("status", "avg_monthly_fee")
    .orderBy("avg_monthly_fee", F.col("status").desc())
)
