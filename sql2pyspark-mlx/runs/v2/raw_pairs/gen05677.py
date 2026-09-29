from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card", "bank_transfer"]) & F.col("order_id").isin([8, 10, 17, 28]))
    .select(
        "id",
        "amount",
        "method",
        "paid_at",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
