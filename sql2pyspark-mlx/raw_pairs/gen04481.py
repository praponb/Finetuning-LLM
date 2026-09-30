from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
    )
)
