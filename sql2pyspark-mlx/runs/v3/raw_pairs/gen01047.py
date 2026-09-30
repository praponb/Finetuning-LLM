from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_time",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
    )
    .orderBy(F.col("amount").desc())
)
