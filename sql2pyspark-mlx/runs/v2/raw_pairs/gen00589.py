from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
    .filter(F.col("total_monthly_fee") > 1340)
)
