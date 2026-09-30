from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["CANCELLED", "EXPIRED", "ACTIVE", "PAUSED"])
    .agg(F.avg("monthly_fee"))
)
