from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1580) | (F.col("status") == "REFUNDED"))
    .select(
        "method",
        "paid_at",
        "order_id",
        F.round((F.col("amount") * 0.9), 2).alias("adjusted_amount"),
        F.date_trunc("month", "paid_at").alias("paid_at_period"),
    )
)
