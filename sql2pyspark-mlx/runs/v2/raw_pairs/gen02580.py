from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .filter(F.col("accounts.customer_id").isin([22, 39]))
    .select(F.col("accounts.id"), F.col("accounts.balance"))
)
