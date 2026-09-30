from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-12-10", "2024-04-01") & (F.col("customer_id") == 25))
    .select(
        "id",
        "start_date",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)
