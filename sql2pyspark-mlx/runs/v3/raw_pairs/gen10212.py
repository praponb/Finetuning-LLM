from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.account_type") == "CHECKING")
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
)
