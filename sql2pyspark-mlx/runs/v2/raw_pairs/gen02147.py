from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["standard", "basic", "enterprise", "premium"])
    .agg(F.avg("monthly_fee"))
    .orderBy("status")
)
