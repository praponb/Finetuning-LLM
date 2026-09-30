from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .select(F.col("accounts.id"), F.col("accounts.customer_id"))
    .orderBy(F.col("customer_id").desc(), F.col("id").desc())
)
