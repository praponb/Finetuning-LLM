from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "PAUSED"])
    .agg(F.sum("monthly_fee"))
)
