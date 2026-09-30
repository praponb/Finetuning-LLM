from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .groupBy("plan")
    .pivot("status", ["CANCELLED", "PAUSED", "ACTIVE", "EXPIRED"])
    .agg(F.count("monthly_fee"))
    .orderBy("plan")
)
