from pyspark.sql import functions as F

result = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.balance") < 16000)
    .select(
        "s.id",
        "s.account_id",
        "s.txn_time",
        "s.merchant",
        "s.is_flagged",
        F.col("p.account_type"),
        F.col("p.balance"),
        F.col("p.opened_date"),
    )
)
