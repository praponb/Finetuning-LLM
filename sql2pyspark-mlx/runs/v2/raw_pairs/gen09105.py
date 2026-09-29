from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 12)
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.max("end_date").alias("latest_end_date"),
    )
)
