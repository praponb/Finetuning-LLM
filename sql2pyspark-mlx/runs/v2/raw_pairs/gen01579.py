from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["EXPIRED", "ACTIVE", "PAUSED"])
    .agg(F.count("monthly_fee"))
    .orderBy("plan")
)
