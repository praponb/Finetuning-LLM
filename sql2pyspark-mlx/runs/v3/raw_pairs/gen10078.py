from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["PAUSED", "CANCELLED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
)
