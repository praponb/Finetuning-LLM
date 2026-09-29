from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .orderBy("monthly_fee_count")
)
