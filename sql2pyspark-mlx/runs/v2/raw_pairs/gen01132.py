from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.opened_date") > "2023-05-01")
    .groupBy(F.col("accounts.status"), F.col("accounts.account_type"))
    .agg(F.countDistinct(F.col("transactions.amount")).alias("unique_accounts"))
)
