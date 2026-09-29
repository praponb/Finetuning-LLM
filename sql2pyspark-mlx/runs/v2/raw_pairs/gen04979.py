from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([8, 22, 40]))
    .select(
        "paid_at",
        "amount",
        "method",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)
