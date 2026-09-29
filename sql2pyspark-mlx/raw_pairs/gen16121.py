from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
    )
    .orderBy("basic_count", "paused_count")
)
