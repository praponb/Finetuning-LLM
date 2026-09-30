from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("a.status"),
        F.when(F.col("t.txn_type").isin(["CREDIT", "DEBIT"]), "Priority").otherwise("Other").alias("txn_type_group"),
    )
    .orderBy("merchant", "txn_type")
)
