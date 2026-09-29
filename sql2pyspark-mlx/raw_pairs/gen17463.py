from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "end_date",
        F.length("status").alias("status_length"),
        F.length("plan").alias("plan_length"),
    )
)
