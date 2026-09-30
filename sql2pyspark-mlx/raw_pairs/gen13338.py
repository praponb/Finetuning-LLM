from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "end_date",
        (F.datediff("end_date", "start_date") / 7).alias("weeks_start_date_to_end_date"),
    )
    .orderBy("weeks_start_date_to_end_date", "end_date", "id")
    .limit(5)
)
