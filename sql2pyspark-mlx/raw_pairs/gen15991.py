from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["standard", "enterprise", "premium", "basic"])
    .agg(F.count("monthly_fee"))
)
