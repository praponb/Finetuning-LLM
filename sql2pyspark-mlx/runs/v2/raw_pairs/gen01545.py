from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-04-01")
    .groupBy("status", F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
    )
    .orderBy(F.col("unique_statuss").desc())
)
