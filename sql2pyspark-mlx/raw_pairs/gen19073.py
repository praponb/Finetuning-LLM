from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

kept_keys = (
    df_transactions
    .select("id")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("avg_transaction_amount"),
    )
    .orderBy("account_id")
)
