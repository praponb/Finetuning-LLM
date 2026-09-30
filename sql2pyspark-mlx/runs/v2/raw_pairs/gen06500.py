from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 24) | (F.year("start_date") == 2024))
    .select(
        "id",
        "status",
        "monthly_fee",
        "plan",
        F.round((F.col("monthly_fee") * 1.1), 2).alias("monthly_fee_adjusted"),
        F.date_sub("start_date", 8).alias("start_date_minus_8d"),
    )
)
