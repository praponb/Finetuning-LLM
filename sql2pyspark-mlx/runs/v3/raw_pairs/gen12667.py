from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.txn_type").isin(["DEBIT", "CREDIT"]) & (F.col("a.status") == "FROZEN"))
    .select(
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.balance"),
    )
)
