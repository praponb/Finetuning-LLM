from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.balance") < 68000)
    .select(F.col("t.txn_type"), F.col("a.customer_id"))
)
