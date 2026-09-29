from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.amount").between(2500, 2700))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.email"),
    )
)
