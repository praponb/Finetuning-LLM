from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 220)
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
    .filter(F.col("premium_monthly_fee") > 26)
    .orderBy(F.col("premium_monthly_fee").desc())
)
