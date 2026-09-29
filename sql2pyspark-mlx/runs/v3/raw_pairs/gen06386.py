from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-07-01", "2024-03-01"))
    .groupBy("plan", "status")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("pct_paused") > 28)
    .orderBy(F.col("pct_paused").desc())
)
