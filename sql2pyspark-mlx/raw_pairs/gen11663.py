from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("a.balance") < 38000) & (F.col("t.amount") >= 1100))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.is_active"),
        F.col("c.country"),
        F.substring(F.col("t.merchant"), 1, 3).alias("merchant_prefix"),
    )
)
