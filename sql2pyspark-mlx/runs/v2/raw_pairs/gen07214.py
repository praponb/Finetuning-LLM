from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-04-01")
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.max("end_date").alias("latest_end_date"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
    .filter(F.col("pct_enterprise") > 37)
)
