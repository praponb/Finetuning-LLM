from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 230))
    .select(
        "start_date",
        "end_date",
        "status",
        F.dayofmonth("start_date").alias("start_date_day"),
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)
