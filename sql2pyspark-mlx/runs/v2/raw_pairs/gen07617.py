from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & (F.year("start_date") >= 2023))
    .select(
        "id",
        "monthly_fee",
        "plan",
        F.date_sub("end_date", 90).alias("end_date_minus_90d"),
    )
)
