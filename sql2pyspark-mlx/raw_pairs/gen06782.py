from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("start_date") >= 2024) | F.month("start_date").isin([3, 4, 11]))
    .select(
        "id",
        "monthly_fee",
        "customer_id",
        "end_date",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)
