from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(190, 220))
    .groupBy("status")
    .pivot("plan", ["enterprise", "premium", "basic"])
    .agg(F.avg("monthly_fee"))
)
