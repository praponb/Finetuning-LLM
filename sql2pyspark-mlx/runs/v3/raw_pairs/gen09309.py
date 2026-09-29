from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.to_date(F.col("t.txn_time")) >= "2025-05-01")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
    .orderBy("opened_date")
)
