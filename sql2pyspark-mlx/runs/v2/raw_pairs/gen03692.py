from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["PAUSED", "EXPIRED", "CANCELLED", "ACTIVE"])
    .agg(F.sum("monthly_fee"))
    .orderBy("plan")
)
