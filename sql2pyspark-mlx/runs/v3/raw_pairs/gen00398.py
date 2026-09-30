from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "method",
        F.length("status").alias("status_length"),
    )
)
