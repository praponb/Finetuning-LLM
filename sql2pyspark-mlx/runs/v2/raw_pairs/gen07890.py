from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "bank_transfer") & (F.col("method") != "card"))
    .select(
        "method",
        "amount",
        "paid_at",
        F.floor("amount").alias("amount_whole"),
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)
