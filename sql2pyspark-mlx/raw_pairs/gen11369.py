from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "customer_id",
        (F.datediff(F.current_date(), "end_date") / 30).alias("months_since_end_date"),
        (F.datediff("end_date", "start_date") / 7).alias("weeks_start_date_to_end_date"),
    )
)
