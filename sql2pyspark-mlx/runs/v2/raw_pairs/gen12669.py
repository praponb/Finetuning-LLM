from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "bank_transfer"]) & F.col("order_id").isNull() & (F.col("status") == "FAILED"))
    .select(
        "paid_at",
        "status",
        "amount",
        F.quarter("paid_at").alias("paid_at_quarter"),
    )
)
