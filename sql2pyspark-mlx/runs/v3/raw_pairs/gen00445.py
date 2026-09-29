from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.account_id").isin([1, 7, 22, 40]) & (F.col("t.txn_time") >= "2023-12-20"))
    .select(
        F.col("t.amount"),
        F.col("a.balance"),
        F.col("c.country"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("balance").desc())
)
