from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["basic", "premium", "enterprise", "standard"])
    .agg(F.avg("monthly_fee"))
)
