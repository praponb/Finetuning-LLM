from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 33)
    .groupBy("plan")
    .pivot("status", ["PAUSED", "ACTIVE"])
    .agg(F.count("monthly_fee"))
)
