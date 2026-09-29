from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("MONTH", "start_date").alias("activity_month"),
        F.when(F.col("monthly_fee") >= 267, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.max(F.when(F.col("plan") == "standard", F.col("monthly_fee"))).alias("standard_max_monthly_fee"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count")
)
