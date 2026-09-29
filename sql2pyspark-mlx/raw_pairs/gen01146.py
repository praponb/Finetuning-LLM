from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("transactions.merchant") == "7-Eleven")
    .select(F.col("transactions.txn_type"), F.col("accounts.account_type"))
)
