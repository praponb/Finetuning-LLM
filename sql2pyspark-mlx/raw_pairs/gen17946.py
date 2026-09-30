from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") <= 71000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(~F.col("t.txn_type").isin(["DEBIT", "REFUND"]))
    .select(F.col("t.id"), F.col("t.txn_type"))
    .orderBy("txn_type", "id")
    .limit(1)
)
