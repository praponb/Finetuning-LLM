from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("end_date") > "2025-07-08") & F.col("status").isin(["ACTIVE", "EXPIRED"]))
)

summary = (
    src
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    summary
    .filter(F.col("cancelled_count") < 76)
    .select("status", "pct_active")
)
