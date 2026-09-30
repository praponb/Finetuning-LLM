from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") < 160)
    .select("plan", "max_monthly_fee")
    .orderBy(F.col("plan").desc())
    .limit(1)
)
