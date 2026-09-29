from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.credit_limit") >= 7500)
    .select(
        F.col("t.txn_type"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.age"),
    )
)
