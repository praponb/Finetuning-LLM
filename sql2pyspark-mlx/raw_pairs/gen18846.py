from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.city"),
    )
    .orderBy("balance")
)
