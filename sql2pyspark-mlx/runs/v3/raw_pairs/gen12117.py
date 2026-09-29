from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("t.merchant").isin(["Grab", "Shell"]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("a.status"),
        F.col("c.is_active"),
        F.col("c.email"),
    )
)
