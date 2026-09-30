from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("opened_date") >= "2025-01-01").alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .filter(F.col("transactions.is_flagged"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_type"),
        F.col("transactions.is_flagged"),
    )
    .orderBy("txn_type")
)
