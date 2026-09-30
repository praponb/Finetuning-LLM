from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.status"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy(F.col("min_amount").desc(), "status")
    .limit(10)
)
