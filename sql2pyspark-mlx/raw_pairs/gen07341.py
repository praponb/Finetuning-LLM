from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.account_id") == 26)
    .select(
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.initcap(F.trim(F.col("t.txn_type"))).alias("txn_type_title"),
    )
)
