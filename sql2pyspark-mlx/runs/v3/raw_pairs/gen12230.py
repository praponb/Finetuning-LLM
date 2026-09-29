from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.country"),
    )
)
