from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.account_id").isin([10, 27]) | (F.col("t.txn_time") < "2024-07-25"))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
