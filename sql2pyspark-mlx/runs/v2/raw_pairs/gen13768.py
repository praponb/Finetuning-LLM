from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer", "promptpay"]))
    .select(
        "amount",
        F.lower("status").alias("status_lower"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy("status_lower", "paid_at_day")
)
