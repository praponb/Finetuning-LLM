from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["PAUSED", "ACTIVE"])
    .agg(F.count("monthly_fee"))
    .orderBy("plan")
)
