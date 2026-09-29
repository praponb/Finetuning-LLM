from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .agg(
        F.count("*").alias("total_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
)
