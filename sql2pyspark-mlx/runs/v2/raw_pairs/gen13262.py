from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.balance") > 26000)
    .select(F.col("a.id"), F.col("a.customer_id"))
)
