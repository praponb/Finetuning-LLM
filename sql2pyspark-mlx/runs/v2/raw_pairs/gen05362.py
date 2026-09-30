from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-24")
    .select(
        "id",
        "paid_at",
        "order_id",
        F.length("status").alias("status_length"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
