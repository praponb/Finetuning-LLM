from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.txn_type") == "CREDIT")
    .select(
        F.col("a.status"),
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.concat(F.col("a.status"), F.lit(" "), F.col("a.account_type")).alias("status_account_type"),
    )
    .orderBy("status_account_type")
)
