from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .filter(~F.col("t.txn_type").isin(["FEE", "CREDIT", "REFUND"]) & F.col("c.is_active"))
    .select(
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("a.balance"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
    .orderBy(F.col("txn_time").desc())
)
