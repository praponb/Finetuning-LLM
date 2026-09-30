from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "start_date",
        "plan",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
    .orderBy("days_end_date_to_start_date")
)
