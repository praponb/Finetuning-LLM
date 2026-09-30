from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise"]))
    .groupBy("status")
    .pivot("plan", ["enterprise", "premium", "standard"])
    .agg(F.count("monthly_fee"))
)
