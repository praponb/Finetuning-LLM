from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & (F.col("plan") == "premium"))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
    )
    .orderBy(F.col("paused_count").desc(), "status")
)
