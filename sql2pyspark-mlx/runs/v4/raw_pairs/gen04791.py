from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("account_id", "id")
    .intersect(
        df_transactions
        .filter(F.hour("txn_time").between(8, 15))
        .select("account_id", "id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "account_id", "amount", "txn_time", "is_flagged")
