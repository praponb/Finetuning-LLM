from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("t.account_id").isin([3, 39]))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.col("c.age"),
    )
)
