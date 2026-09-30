from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date") <= "2024-06-01")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
    )
)
