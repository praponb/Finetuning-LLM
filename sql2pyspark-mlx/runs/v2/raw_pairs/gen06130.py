from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.txn_type") != "DEBIT")
    .select(
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
    )
    .orderBy("merchant")
)
