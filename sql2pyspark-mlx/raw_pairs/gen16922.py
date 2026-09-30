from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .select(
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.age"),
    )
)
