from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["premium", "basic", "standard"])
    .agg(F.avg("monthly_fee"))
)
