from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.account_id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("c.age"),
        F.col("c.name"),
    )
)
