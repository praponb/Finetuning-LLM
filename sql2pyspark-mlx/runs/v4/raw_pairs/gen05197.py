from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 56)
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy("account_type", "min_amount")
)
