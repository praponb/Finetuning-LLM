from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2024-04-30 18:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws("/", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

result = (
    filtered
    .groupBy("order_id", "status")
    .pivot("method", ["cash", "bank_transfer", "paypal"])
    .agg(F.avg("amount").alias("amount"))
)
