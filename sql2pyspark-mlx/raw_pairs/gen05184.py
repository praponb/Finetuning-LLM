from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("t.txn_time"),
        F.col("a.status"),
        F.col("c.is_active"),
        F.col("c.city"),
    )
)
