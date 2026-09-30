from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 35) & (F.year("end_date") >= 2024))
    .select(
        "plan",
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)
