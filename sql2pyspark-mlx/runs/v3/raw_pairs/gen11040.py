from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.account_id").isin([22, 36]))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy("name", "id")
    .limit(5)
)
