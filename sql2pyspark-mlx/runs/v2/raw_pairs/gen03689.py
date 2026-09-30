from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "EXPIRED", "CANCELLED"])
    .agg(F.count("monthly_fee"))
)
