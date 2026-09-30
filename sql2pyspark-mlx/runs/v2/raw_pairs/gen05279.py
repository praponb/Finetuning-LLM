from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-11-01") & (F.col("method") == "card"))
    .select(
        "status",
        "amount",
        "order_id",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
    .orderBy("status", "order_id")
)
