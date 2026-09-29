from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("merchant", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([5, 8, 10, 15]))
        .select("merchant", "id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("account_id", "txn_type", "amount", "txn_time", "merchant")
