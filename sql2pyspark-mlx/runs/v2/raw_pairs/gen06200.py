from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "cash"]))
    .select(
        "amount",
        "order_id",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
    .orderBy(F.col("days_since_paid_at").desc())
)
