from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("customer_id").isin([3, 19, 29]) & (F.col("status") == "PAUSED") & (F.col("end_date") < "2023-03-01")))
    .select(
        "id",
        "status",
        "customer_id",
        "start_date",
        F.last_day("start_date").alias("start_date_month_end"),
        F.trim("status").alias("clean_status"),
    )
    .orderBy("start_date", "id")
    .limit(5)
)
