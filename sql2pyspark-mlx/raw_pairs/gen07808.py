from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("plan", ["premium", "enterprise", "basic"])
    .agg(F.max("monthly_fee"))
    .orderBy("customer_id")
)
