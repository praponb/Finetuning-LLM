from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy(F.col("total_monthly_fee").desc())
)
