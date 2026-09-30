from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") > 220)
    .select("plan", "max_monthly_fee", "min_monthly_fee")
    .orderBy("plan", "min_monthly_fee")
    .limit(50)
)
