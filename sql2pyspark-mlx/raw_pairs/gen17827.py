from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 1800) | (F.col("amount") >= 3000))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
    )
)
