from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .filter((F.col("t.amount") <= 1500) & F.col("a.status").isNull())
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc())
)
