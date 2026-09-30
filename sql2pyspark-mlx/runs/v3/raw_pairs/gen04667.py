from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & (F.col("start_date") < "2025-05-06"))
    .select(
        "id",
        "customer_id",
        "start_date",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
