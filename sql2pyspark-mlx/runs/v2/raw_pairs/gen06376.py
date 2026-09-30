from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "plan",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.length("status").alias("status_length"),
    )
    .orderBy(F.col("days_since_end_date").desc())
)
