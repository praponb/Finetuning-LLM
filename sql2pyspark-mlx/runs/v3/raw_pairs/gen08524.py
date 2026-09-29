from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .pivot("status", ["CANCELLED", "PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee"))
    .orderBy("end_quarter")
)
