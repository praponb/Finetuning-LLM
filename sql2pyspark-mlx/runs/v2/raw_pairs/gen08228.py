from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "plan",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.date_sub("end_date", 69).alias("end_date_minus_69d"),
    )
)
