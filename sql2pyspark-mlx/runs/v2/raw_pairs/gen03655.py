from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & F.col("order_id").isin([3, 18, 28]) & (F.col("paid_at") >= "2025-10-01"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
    .orderBy(F.col("paid_at_date").desc(), "id")
    .limit(3)
)
