from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-04-29", "2025-05-01"))
    .groupBy(
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)
