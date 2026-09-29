from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-07-18", "2024-07-01") | F.col("monthly_fee").isNotNull())
    .select(
        "customer_id",
        F.add_months("end_date", 1).alias("end_date_plus_months"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)
