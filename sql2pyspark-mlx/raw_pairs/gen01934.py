from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-10-01")
    .select(
        "order_id",
        F.substring("method", 1, 2).alias("method_prefix"),
        F.trim("status").alias("clean_status"),
    )
)
