from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .filter(F.col("transactions.account_id").isin([2, 23]))
    .groupBy(F.date_trunc("MONTH", F.col("accounts.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("transactions.amount")).alias("min_amount"))
    .orderBy("opened_month")
    .limit(3)
)
