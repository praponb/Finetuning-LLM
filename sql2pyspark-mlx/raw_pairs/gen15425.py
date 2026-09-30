from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-10").cast("date")) & F.col("account_id").isin([2, 12, 28]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        "src.is_flagged",
        "src.is_flagged_label",
        F.col("p.opened_date"),
        F.col("p.account_type"),
        F.col("p.balance"),
    )
)
