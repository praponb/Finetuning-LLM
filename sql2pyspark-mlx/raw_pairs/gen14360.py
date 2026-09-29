from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("a.status"),
        F.col("t.txn_type"),
        F.when(F.col("a.balance") >= 66000, "High").when(F.col("a.balance") >= 25000, "Medium").otherwise("Low").alias("balance_band"),
    )
)
