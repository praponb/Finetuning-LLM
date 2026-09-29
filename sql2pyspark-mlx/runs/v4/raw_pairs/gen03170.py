from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("paid_at") >= "2025-02-01") & F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "amount",
        "method",
        "order_id",
        F.date_trunc("month", "paid_at").alias("paid_at_period"),
        F.date_trunc("week", "paid_at").alias("paid_at_period_2"),
    )
    .orderBy("order_id", "amount")
)
