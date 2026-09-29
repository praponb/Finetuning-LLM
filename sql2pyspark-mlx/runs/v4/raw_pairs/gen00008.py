from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["EXPIRED", "CANCELLED", "PAUSED", "ACTIVE"])
    .agg(F.count("monthly_fee"))
)
