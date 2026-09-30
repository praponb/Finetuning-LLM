from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", "status")
    .pivot("plan", ["basic", "premium"])
    .agg(F.max("monthly_fee").alias("value_sum"))
)
