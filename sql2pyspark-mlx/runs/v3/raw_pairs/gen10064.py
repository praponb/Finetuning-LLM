from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("customer_id").isin([3, 16, 28])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .filter(F.col("transactions.txn_time") >= "2023-06-01")
    .select(F.col("transactions.id"), F.col("transactions.amount"))
    .orderBy(F.col("amount").desc())
)
