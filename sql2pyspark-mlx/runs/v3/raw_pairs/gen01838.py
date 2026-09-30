from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan", F.month("end_date").alias("end_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("*").alias("subscription_count"),
    )
)
