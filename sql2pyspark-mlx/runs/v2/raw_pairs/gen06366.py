from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .groupBy(
        F.when(F.col("monthly_fee") >= 195, "high").when(F.col("monthly_fee") >= 37, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") > 1170)
    .select("monthly_fee_bucket", "total_monthly_fee")
    .orderBy(F.col("monthly_fee_bucket").desc())
)
