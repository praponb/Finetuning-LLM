from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .filter(F.col("c.city") == "Singapore")
    .select(
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.email"),
    )
    .orderBy("status")
)
