from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .select(
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
