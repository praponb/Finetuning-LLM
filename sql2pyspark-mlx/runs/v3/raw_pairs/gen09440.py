from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.hour(F.col("t.txn_time")).between(12, 15) & (F.col("c.segment") != "Corporate"))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.age"),
    )
)
