from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 11, 23]))
    .select(
        "id",
        "paid_at",
        "amount",
        "method",
        F.lower("status").alias("status_lower"),
    )
)
