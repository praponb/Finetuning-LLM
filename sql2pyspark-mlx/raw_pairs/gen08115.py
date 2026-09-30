from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.account_type") == "CREDIT")
    .select(
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)
