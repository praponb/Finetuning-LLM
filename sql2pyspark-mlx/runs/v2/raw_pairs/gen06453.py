from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status").isNull()).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("t.amount"),
    )
)
