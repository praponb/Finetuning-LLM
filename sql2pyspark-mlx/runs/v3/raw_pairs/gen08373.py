from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.when(F.col("monthly_fee") >= 229, "high").when(F.col("monthly_fee") >= 50, "mid").otherwise("low").alias("monthly_fee_bucket"))
    .pivot("status", ["ACTIVE", "EXPIRED"])
    .agg(F.sum("monthly_fee"))
    .orderBy("monthly_fee_bucket")
)
