from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("amount") <= 410) & ((F.col("order_id") == 18) | (F.to_date("paid_at") < "2024-12-01"))))
    .select(
        "id",
        "order_id",
        "status",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
    )
)
