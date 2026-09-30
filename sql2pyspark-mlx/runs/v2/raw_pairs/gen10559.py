from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.account_type").isin(["LOAN", "SAVINGS"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
    .orderBy("account_id")
)
