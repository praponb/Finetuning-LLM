from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.year("end_date").alias("end_year"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
