from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.substring(F.col("a.account_type"), 1, 4).alias("account_type_prefix"),
    )
    .orderBy("txn_time", "txn_type")
)
