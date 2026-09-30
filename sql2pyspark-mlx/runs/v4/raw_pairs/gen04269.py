from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["PAUSED", "ACTIVE", "EXPIRED", "CANCELLED"])
    .agg(F.max("monthly_fee"))
    .orderBy("plan")
)
