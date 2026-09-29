from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .orderBy(F.col("first_start_date").desc())
)
