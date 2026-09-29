from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") >= 70)
    .select("status", "plan", "min_monthly_fee")
    .orderBy(F.col("min_monthly_fee").desc())
)
