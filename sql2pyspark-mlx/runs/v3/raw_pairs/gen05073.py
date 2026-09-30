from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(~F.col("a.account_type").isin(["SAVINGS", "CREDIT"]) | (F.col("t.amount") >= 2700))
    .select(
        F.col("a.balance"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("t.account_id"),
        F.trim(F.col("t.txn_type")).alias("clean_txn_type"),
    )
)
