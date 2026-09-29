from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "plan",
        "monthly_fee",
        F.date_trunc("YEAR", "end_date").alias("end_date_period"),
    )
)
