from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

matched_keys = (
    df_transactions
    .select("id", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant").isin(["Shell", "Amazon", "Grab"]))
        .select("id", "txn_type")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select(
        "src.account_id",
        "src.txn_type",
        "src.merchant",
        "src.is_flagged",
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)

remaining_keys = (
    with_dims
    .select("merchant")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([2, 25]))
        .select("merchant")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("merchant", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT"]))
        .select("merchant", "account_id")
    )
)

reconciled_transactions = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

matched_keys_3 = (
    reconciled_transactions
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.to_date("txn_time") < "2023-04-16")
        .select("txn_type")
    )
)

reconciled_transactions_2 = (
    reconciled_transactions
    .alias("s")
    .join(matched_keys_3.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    reconciled_transactions_2
    .join(amount_by_account_id, on="account_id", how="left")
    .select("account_id", "merchant", "opened_date", F.col("max_transaction_amount"))
)
