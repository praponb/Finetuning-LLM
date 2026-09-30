from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.segment"),
        F.length(F.col("t.txn_type")).alias("txn_type_length"),
    )
)
