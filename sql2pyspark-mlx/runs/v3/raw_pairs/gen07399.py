from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 140)
    .select(
        "start_date",
        "monthly_fee",
        "end_date",
        F.last_day("start_date").alias("start_date_month_end"),
    )
    .orderBy("start_date", "end_date")
)
