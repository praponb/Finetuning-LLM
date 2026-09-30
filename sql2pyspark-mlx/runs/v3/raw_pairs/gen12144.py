from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.merchant").isin(["Grab", "Lazada"]))
    .select(
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.amount"),
        F.col("a.account_type"),
    )
)
