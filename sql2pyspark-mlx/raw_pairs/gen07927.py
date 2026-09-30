from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant") == "7-Eleven")
    .select(
        F.col("a.status"),
        F.col("t.txn_type"),
        F.col("t.account_id"),
        F.col("t.merchant"),
    )
    .orderBy(F.col("merchant").desc())
)
