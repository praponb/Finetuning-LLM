from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .filter(F.col("a.balance").between(34000, 44000))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
