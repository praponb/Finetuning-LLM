from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("a.status"),
    )
    .orderBy(F.col("amount").desc())
)
