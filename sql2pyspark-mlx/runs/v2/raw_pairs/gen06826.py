from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("t.txn_type").isin(["DEBIT", "FEE"]) & (F.col("a.balance") > 42000))
    .select(
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
    )
    .orderBy("amount")
)
