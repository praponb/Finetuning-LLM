from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") == "LOAN")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        "src.is_flagged",
        F.col("a.status"),
    )
)
