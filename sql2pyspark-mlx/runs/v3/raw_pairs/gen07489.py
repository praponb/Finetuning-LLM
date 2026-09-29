from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
    .filter(F.col("premium_count") >= 9)
    .orderBy("plan")
)
