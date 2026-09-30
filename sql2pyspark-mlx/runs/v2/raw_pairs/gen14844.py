from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.balance") >= 53000) & F.col("a.customer_id").isNotNull())
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("c.segment"),
    )
)
