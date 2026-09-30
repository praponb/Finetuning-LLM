from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("a.status").isin(["CLOSED", "FROZEN"]) & F.col("c.is_active"))
    .select(F.col("t.id"), F.col("t.txn_type"), F.col("a.account_type"), F.col("c.age"))
    .orderBy(F.col("age").desc())
)
