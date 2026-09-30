from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .groupBy(F.col("customers.city"))
    .agg(F.round(F.avg(F.col("transactions.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 71)
)
