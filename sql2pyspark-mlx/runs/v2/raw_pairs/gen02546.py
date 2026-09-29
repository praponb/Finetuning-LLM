from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
)
