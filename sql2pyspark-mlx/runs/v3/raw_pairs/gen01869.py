from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & ((F.col("monthly_fee") < 80) | ~F.col("status").isin(["CANCELLED", "ACTIVE", "PAUSED"])))
    .groupBy("plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") >= 110)
    .orderBy(F.col("plan").desc())
    .limit(20)
)
