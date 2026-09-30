from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.to_date(F.col("t.txn_time")) < "2024-08-01")
    .groupBy(F.col("c.city"))
    .agg(F.count("*").alias("cnt"))
)
