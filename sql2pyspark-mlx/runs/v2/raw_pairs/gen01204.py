from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "order_id",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.lower("status").alias("status_lower"),
    )
)
