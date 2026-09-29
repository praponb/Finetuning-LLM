from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]))
    .groupBy("plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") >= 70)
    .orderBy(F.col("max_monthly_fee").desc(), "plan")
    .limit(1)
)
