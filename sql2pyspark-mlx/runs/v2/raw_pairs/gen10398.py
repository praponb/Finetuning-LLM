from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.year("end_date") == 2024) | (F.year("start_date") == 2023))
)

start_quarter_summary = (
    src
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
)

result = start_quarter_summary.filter(F.col("active_count") > 53)
