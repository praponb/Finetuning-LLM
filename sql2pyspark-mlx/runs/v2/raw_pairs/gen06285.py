from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("paid_at") >= "2024-03-01") & F.col("order_id").isin([6, 30, 32])) | (F.col("amount") <= 790))
    .select(
        "amount",
        "status",
        F.substring("method", 1, 4).alias("method_prefix"),
        F.substring("status", 1, 3).alias("status_prefix"),
    )
    .orderBy("method_prefix", "status_prefix")
)
