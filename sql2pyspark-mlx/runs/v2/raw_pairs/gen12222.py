from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(960, 1220))
    .select(
        "id",
        "amount",
        "method",
        "order_id",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
