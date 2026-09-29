from pyspark.sql import functions as F

amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-23 15:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type").isin(["LOAN", "CHECKING", "SAVINGS"]))
    .select("src.*", F.col("a.opened_date"))
)

matched_keys = (
    joined
    .select("txn_type", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("txn_type", "account_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.txn_time",
        "s.merchant",
        "s.is_flagged_label",
        "s.opened_date",
        F.col("d.status"),
        F.col("d.opened_date").alias("account_opened_date"),
        F.col("d.balance"),
    )
)

result = (
    with_dims
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "opened_date",
        "status",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("txn_type").desc())
)
