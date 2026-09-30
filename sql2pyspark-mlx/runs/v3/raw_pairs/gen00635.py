from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
    .orderBy("status", F.col("active_count").desc())
)
