from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("account_id").isin([21, 29]) & (F.col("txn_time") >= F.lit("2023-08-21 00:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

result = (
    valid
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)
