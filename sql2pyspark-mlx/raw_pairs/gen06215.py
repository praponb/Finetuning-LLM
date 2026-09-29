from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") >= 4)
    .orderBy("status")
)
