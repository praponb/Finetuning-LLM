from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        (F.datediff("start_date", "end_date") / 7).alias("weeks_end_date_to_start_date"),
    )
)
