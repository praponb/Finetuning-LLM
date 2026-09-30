from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 70)
    .select(
        "id",
        "plan",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)
