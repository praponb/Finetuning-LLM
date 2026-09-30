from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
    )
    .orderBy("pct_standard", "status")
    .limit(20)
)
