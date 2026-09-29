from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium", "enterprise"]))
    .groupBy("customer_id")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") < 1420)
    .select("customer_id", "total_monthly_fee")
)
