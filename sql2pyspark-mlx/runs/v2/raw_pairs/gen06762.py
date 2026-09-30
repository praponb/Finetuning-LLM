from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("transactions.account_id").isin([16, 19, 32, 40]) & (F.col("accounts.balance") > 62000))
    .select(
        F.col("transactions.amount"),
        F.col("transactions.is_flagged"),
        F.col("transactions.txn_time"),
        F.col("accounts.status"),
        F.length(F.col("transactions.merchant")).alias("merchant_length"),
    )
    .orderBy("amount")
)
