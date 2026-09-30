from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 32)
    .groupBy("status")
    .pivot("plan", ["basic", "enterprise", "standard"])
    .agg(F.sum("monthly_fee"))
    .orderBy("status")
)
