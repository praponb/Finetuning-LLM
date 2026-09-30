from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-05-01")
    .select(
        "id",
        "paid_at",
        "status",
        "order_id",
        F.length("status").alias("status_length"),
    )
)
