from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 11) & ((F.col("monthly_fee") > 180) | (F.col("status") == "PAUSED")))
    .select(
        "monthly_fee",
        "customer_id",
        "status",
        F.lower("status").alias("status_lower"),
    )
)
