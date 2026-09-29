from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("opened_date").between("2023-08-17", "2023-12-01")).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_type"),
        F.col("transactions.is_flagged"),
        F.col("transactions.account_id"),
    )
)
