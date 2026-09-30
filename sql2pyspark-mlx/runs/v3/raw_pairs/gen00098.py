from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_type"),
    )
    .orderBy("amount")
)
