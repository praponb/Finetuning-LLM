from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube("customer_id", F.date_trunc("YEAR", "start_date").alias("year_start"), "plan")
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_rows"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_total_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
