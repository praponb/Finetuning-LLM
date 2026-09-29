from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("MONTH", "start_date").alias("activity_month"),
        "status",
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
