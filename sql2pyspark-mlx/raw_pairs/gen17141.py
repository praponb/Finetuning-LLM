from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2024-03-02") & F.col("method").isin(["promptpay", "card", "bank_transfer"]))
    .select(
        "amount",
        "order_id",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("amount_int").desc(), "order_id")
)
