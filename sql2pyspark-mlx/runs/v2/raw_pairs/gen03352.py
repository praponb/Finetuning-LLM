from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube("customer_id", "status")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .orderBy("status")
)
