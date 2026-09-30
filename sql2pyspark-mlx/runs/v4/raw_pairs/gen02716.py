from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") < "2025-03-01")
        .select("txn_type", "account_id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("amount", "txn_time", "merchant")
    .orderBy(F.col("merchant").desc(), F.col("amount").desc())
)
