from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("transactions.is_flagged"))
    .groupBy(F.col("accounts.status"))
    .agg(F.round(F.stddev(F.col("transactions.amount")), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 61)
    .orderBy("status")
    .limit(3)
)
