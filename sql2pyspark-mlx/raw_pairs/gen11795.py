from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment") == "Corporate")
    .select(
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.balance"),
        F.col("c.segment"),
    )
)
