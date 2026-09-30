from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("account_type").isin(["CREDIT", "SAVINGS"])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .filter(F.col("transactions.account_id").isin([7, 26]))
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.merchant"),
        F.col("transactions.txn_type"),
    )
    .orderBy("txn_type")
)
