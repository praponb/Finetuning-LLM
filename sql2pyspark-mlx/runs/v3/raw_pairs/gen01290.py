from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .groupBy("status", F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .orderBy(F.col("start_quarter").desc())
)
