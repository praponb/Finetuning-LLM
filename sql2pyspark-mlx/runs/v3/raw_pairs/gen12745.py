from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
)
