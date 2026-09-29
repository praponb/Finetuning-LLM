from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
    )
    .orderBy(F.col("txn_type").desc(), "account_type")
)
