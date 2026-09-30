from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter((F.col("a.balance") >= 31000) & (F.col("a.opened_date") <= "2024-06-28"))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("a.status"),
    )
    .orderBy("account_id")
)
