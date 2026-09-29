from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 3)
    .groupBy("status", F.month("start_date").alias("start_month"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("monthly_fee_count") >= 1)
)
