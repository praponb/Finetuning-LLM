from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
    .filter(F.col("monthly_fee_count") >= 5)
)
