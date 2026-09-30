from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "ACTIVE")
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("end_month").desc())
)
