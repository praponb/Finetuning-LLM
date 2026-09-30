from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee"))
    .orderBy("plan")
)
