from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.is_active") & (F.col("t.amount") < 1800))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.txn_time"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.country"),
    )
    .orderBy("txn_time")
)
