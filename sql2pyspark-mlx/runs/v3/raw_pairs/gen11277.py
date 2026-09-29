from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "PAUSED", "EXPIRED"]) & (F.col("status") == "ACTIVE"))
    .groupBy("customer_id")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)
