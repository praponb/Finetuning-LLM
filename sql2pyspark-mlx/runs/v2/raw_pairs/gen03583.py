from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1450, 1550) & (F.col("paid_at") >= "2024-08-01"))
    .select(
        "paid_at",
        "order_id",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
)
