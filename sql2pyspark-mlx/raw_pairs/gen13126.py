from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.txn_type").isin(["FEE", "DEBIT", "REFUND"]))
    .select(F.col("t.merchant"), F.col("t.account_id"), F.col("a.customer_id"))
    .orderBy("account_id")
)
