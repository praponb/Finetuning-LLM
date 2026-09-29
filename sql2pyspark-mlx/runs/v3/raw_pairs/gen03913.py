from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2024-07-28")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
    )
    .orderBy("total_monthly_fee", F.col("pct_premium").desc())
)
