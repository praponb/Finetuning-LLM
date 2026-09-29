from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
    .orderBy("txn_time")
)
