from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .orderBy(F.col("pct_expired").desc())
)
