from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status").isNotNull() & (F.col("t.account_id") == 22))
    .select(
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("c.country"),
    )
)
