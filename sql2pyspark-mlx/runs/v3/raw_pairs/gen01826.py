from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 196, "high").when(F.col("monthly_fee") >= 41, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") >= 190)
    .select("monthly_fee_bucket", "min_monthly_fee")
    .orderBy("monthly_fee_bucket")
    .limit(3)
)
