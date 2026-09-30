from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "CANCELLED", "PAUSED"])
    .agg(F.sum("monthly_fee"))
)
