from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.amount").between(1500, 2600))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
    )
)
