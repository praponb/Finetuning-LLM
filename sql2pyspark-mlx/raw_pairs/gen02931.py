from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
)
