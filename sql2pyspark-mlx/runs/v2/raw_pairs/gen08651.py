from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("account_type") != "CREDIT").alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_type"),
        F.col("transactions.is_flagged"),
    )
)
