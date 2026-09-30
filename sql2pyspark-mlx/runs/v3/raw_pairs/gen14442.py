from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") <= 4500)
    .groupBy(
        F.when(F.col("c.credit_limit") >= 12700, "high").when(F.col("c.credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
