from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 170))
    .groupBy("plan")
    .pivot("status", ["PAUSED", "EXPIRED", "ACTIVE"])
    .agg(F.sum("monthly_fee"))
)
