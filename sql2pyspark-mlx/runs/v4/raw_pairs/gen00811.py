from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") <= "2024-10-01"))
    .select("*", F.date_trunc("week", "start_date").alias("start_date_period"))
)

result = (
    valid
    .groupBy("plan", F.date_trunc("WEEK", "start_date_period").alias("week_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        ((F.unix_timestamp(F.max("start_date_period")) - F.unix_timestamp(F.min("start_date_period"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee"))).alias("basic_total_monthly_fee"),
    )
)
