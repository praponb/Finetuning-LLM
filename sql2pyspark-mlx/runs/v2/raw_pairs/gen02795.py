from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1020) & F.col("method").isin(["cash", "paypal"]) & F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select(
        "id",
        "status",
        "method",
        "amount",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
    .orderBy(F.col("days_since_paid_at").desc(), "id")
    .limit(3)
)
