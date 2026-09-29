from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["basic", "standard", "premium"])
    .agg(F.count("monthly_fee"))
    .orderBy("status")
)
