from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & F.col("order_id").isin([10, 29, 32]) & F.col("method").isin(["promptpay", "paypal"]))
    .select(
        "id",
        "status",
        "amount",
        "method",
        F.round("amount", 1).alias("amount_rounded"),
    )
    .orderBy(F.col("amount").desc())
)
