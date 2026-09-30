from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "EXPIRED"])
    .agg(F.max("monthly_fee"))
)
