from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "EXPIRED", "CANCELLED", "PAUSED"])
    .agg(F.max("monthly_fee"))
    .orderBy("plan")
)
