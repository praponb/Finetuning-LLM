from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["enterprise", "standard", "basic"])
    .agg(F.sum("monthly_fee"))
)
