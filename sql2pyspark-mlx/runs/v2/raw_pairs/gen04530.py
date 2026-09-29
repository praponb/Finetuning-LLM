from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 120)
    .groupBy("status", "plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") >= 90)
    .orderBy(F.col("min_monthly_fee").desc(), "status", "plan")
    .limit(10)
)
