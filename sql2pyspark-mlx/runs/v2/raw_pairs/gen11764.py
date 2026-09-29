from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter((F.col("a.status") != "FROZEN") | (F.col("a.status") == "CLOSED"))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
    .orderBy("customer_id")
)
