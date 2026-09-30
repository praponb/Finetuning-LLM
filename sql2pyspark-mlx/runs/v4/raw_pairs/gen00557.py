from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("paid_at") < "2023-09-02") & (F.to_date("paid_at") < "2025-01-01"))
    .select(
        "amount",
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)
