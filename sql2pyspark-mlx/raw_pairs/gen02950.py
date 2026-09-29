from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & ~F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "id",
        "order_id",
        "paid_at",
        "method",
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("amount_int").desc(), "id")
    .limit(10)
)
