from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.month("start_date").alias("start_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("pct_active") >= 57)
    .orderBy(F.col("start_month").desc())
)
