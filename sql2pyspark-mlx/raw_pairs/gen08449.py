from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & (F.col("end_date") <= "2025-02-08"))
    .select(
        "monthly_fee",
        "start_date",
        "plan",
        F.last_day("start_date").alias("start_date_month_end"),
        F.dayofweek("end_date").alias("end_date_dayofweek"),
    )
)
