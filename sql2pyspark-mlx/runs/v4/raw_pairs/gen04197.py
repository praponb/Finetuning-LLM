from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~((F.col("t.txn_type") == "REFUND") | (F.col("t.amount") < 1400)))
    .select(
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.opened_date"),
    )
)
