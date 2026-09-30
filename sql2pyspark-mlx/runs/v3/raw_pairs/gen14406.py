from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("t.amount") < 3100) & (F.col("a.balance") >= 31000))
    .select(
        F.col("t.amount"),
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
)
