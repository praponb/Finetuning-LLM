from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

matched_keys = (
    df_transactions
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(~F.col("merchant").isin(["Grab", "7-Eleven"]))
        .select("id", "account_id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
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
        F.when(F.col("txn_type").rlike("^(FEE|CREDIT)$"), "PRIORITY").when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "GROUP_B").otherwise("EXTENDED").alias("txn_type_zone"),
        F.col("avg_transaction_amount"),
    )
)
