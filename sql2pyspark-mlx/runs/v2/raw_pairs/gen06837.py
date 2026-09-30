from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "DEBIT").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.balance") > 34000)
    .select(F.col("a.id"), F.col("a.balance"))
)
