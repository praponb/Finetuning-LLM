from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium", "enterprise"]))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)
