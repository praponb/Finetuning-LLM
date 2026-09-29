from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)
