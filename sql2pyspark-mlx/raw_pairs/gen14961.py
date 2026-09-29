from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("start_date").alias("last_start_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy("plan")
)
