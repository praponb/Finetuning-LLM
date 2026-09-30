from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("customer_id").isNull()).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .filter(F.col("transactions.account_id") == 32)
    .select(F.col("transactions.id"), F.col("transactions.amount"))
)
