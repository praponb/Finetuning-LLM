from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2025-06-01")
    .select(
        "id",
        "amount",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
    )
)
