from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "monthly_fee",
        "end_date",
        "customer_id",
        F.add_months("start_date", 12).alias("start_date_plus_months"),
    )
    .orderBy(F.col("start_date_plus_months").desc())
)
