from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("*").alias("num_subscriptions"),
    )
    .orderBy("plan")
)
