from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-02-11")
    .groupBy("plan")
    .pivot("status", ["PAUSED", "CANCELLED", "EXPIRED"])
    .agg(F.count("monthly_fee"))
    .orderBy("plan")
)
