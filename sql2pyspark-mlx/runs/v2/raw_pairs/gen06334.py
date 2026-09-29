from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.amount").between(1700, 2100))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("c.age"),
        F.col("c.segment"),
    )
)
