from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "PAUSED") | F.col("customer_id").isNull())
    .select(
        "id",
        "monthly_fee",
        F.add_months("start_date", 6).alias("start_date_plus_months"),
    )
)
