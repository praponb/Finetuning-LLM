from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .groupBy(F.col("customers.country"), F.col("customers.city"))
    .agg(F.sum(F.col("transactions.amount")).alias("total_amount"))
)
