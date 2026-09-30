from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("start_date").alias("latest_start_date"),
    )
)
