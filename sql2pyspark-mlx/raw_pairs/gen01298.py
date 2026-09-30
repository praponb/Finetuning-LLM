from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("start_date") < "2023-06-01") & (F.year("start_date") == 2025))
)

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
)

result = (
    status_summary
    .select("status", "pct_enterprise")
    .orderBy("status", F.col("pct_enterprise").desc())
)
