from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.year(F.col("a.opened_date")) >= 2024) & (F.col("c.credit_limit") <= 7400))
    .select(
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
)
