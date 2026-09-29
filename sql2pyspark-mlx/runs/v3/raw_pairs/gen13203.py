from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["basic", "premium", "enterprise"])
    .agg(F.sum("monthly_fee"))
)
