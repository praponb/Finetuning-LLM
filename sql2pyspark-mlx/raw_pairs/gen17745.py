from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2023-10-23") & (F.col("order_id") == 11) & (F.col("paid_at") < "2025-10-03"))
    .select(
        "id",
        "amount",
        "paid_at",
        F.round("amount", 0).alias("amount_rounded"),
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)
