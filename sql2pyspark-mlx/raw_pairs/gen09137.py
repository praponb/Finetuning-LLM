from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
    )
    .orderBy(F.col("total_count").desc())
)
