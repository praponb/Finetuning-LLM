from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
    )
    .orderBy("txn_type")
)
