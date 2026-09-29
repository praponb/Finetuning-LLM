from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .filter(F.col("t.txn_time") < "2023-10-01")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
        F.when(F.col("a.account_type").isin(["LOAN", "SAVINGS"]), "Core").otherwise("Other").alias("account_type_group"),
    )
    .orderBy("txn_time", F.col("account_type").desc())
)
