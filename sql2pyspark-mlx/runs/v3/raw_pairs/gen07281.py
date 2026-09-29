from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["ACTIVE", "CANCELLED", "PAUSED"])
    .agg(F.avg("monthly_fee"))
    .orderBy("customer_id")
)
