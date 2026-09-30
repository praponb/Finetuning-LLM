from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.age").between(39, 53))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
    )
)
