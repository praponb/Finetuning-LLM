from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["enterprise", "premium", "basic"])
    .agg(F.avg("monthly_fee"))
)
