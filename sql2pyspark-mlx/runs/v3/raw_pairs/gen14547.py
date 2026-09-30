from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.when(F.col("monthly_fee") >= 202, "high").when(F.col("monthly_fee") >= 103, "mid").otherwise("low").alias("monthly_fee_bucket"))
    .pivot("status", ["ACTIVE", "CANCELLED", "EXPIRED", "PAUSED"])
    .agg(F.avg("monthly_fee"))
)
