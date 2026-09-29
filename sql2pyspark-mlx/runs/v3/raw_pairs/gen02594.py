from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.opened_date") > "2025-01-01")
    .select(
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)
