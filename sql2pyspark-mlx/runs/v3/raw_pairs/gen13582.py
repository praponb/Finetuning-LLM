from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard"]) & F.col("customer_id").isin([12, 13]))
    .groupBy("status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)
