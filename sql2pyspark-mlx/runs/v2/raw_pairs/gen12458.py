from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-04-16", "2023-06-18"))
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
