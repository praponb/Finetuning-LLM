from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") >= 220)
    .select("status", "customer_id", "min_monthly_fee")
    .orderBy(F.col("min_monthly_fee").desc(), F.col("customer_id").desc())
)
