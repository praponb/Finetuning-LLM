from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-06-10")
    .select(
        "id",
        "customer_id",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
