from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") >= 50)
    .select("plan", "avg_monthly_fee")
)
