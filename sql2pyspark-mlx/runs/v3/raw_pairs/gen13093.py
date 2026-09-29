from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-04-29")
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_monthly_fee") < 200)
)
