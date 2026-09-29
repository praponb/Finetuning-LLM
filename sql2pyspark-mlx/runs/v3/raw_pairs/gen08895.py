from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("t.account_id").isin([18, 22, 25, 30]) & (F.col("t.amount") >= 3700))
    .select(
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.account_type"),
        F.col("c.signup_date"),
    )
    .orderBy("signup_date", "txn_time")
)
