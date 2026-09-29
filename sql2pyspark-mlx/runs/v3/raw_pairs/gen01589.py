from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-05-01") | (F.col("method") == "paypal"))
    .select(
        "id",
        "paid_at",
        "method",
        "order_id",
        F.upper("status").alias("status_upper"),
    )
    .orderBy("order_id", "id")
    .limit(3)
)
