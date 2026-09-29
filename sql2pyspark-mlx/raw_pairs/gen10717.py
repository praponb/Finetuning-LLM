from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("account_id", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([1, 14, 35]))
        .select("account_id", "txn_type")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "account_id", "txn_type", "txn_time", "merchant", "is_flagged")
)
