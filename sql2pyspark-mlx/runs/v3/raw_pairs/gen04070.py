from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.length(F.col("c.email")) >= 8)
    .groupBy(F.col("a.account_type"))
    .agg(F.sum(F.col("t.amount")).alias("total_amount"))
    .filter(F.col("total_amount") >= 23000)
)
