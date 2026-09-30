from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .pivot("plan", ["premium", "enterprise"])
    .agg(F.max("monthly_fee"))
)
