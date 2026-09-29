from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy("status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)
