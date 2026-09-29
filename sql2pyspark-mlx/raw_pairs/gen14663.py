from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"), F.col("c.country"))
    .agg(F.max(F.col("t.amount")).alias("max_amount"))
    .filter(F.col("max_amount") >= 1400)
    .orderBy(F.col("country").desc())
)
