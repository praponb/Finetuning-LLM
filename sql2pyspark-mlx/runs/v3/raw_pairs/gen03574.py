from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("customer_id").isin([26, 39])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_semi")
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("transactions.txn_type"),
    )
    .orderBy(F.col("txn_type").desc())
)
