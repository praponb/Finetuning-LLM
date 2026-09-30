from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .filter(F.col("premium_count") < 37)
    .orderBy(F.col("expired_count").desc())
)
