from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("amount") >= 1150))
    .select(
        "order_id",
        "amount",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)
