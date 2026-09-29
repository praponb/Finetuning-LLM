from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "enterprise", "standard"]))
    .groupBy("status")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") > 80)
    .select("status", "max_monthly_fee")
    .orderBy("status")
    .limit(1)
)
