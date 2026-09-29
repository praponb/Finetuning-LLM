from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 80)
    .select(
        "id",
        "start_date",
        F.date_trunc("WEEK", "start_date").alias("start_date_period"),
    )
)
