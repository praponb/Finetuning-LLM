from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("a.opened_date"),
    )
    .orderBy(F.col("txn_type").desc())
)
