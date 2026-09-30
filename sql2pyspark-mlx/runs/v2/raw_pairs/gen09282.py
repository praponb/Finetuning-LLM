from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("transactions.account_id"),
    )
    .orderBy("customer_id")
)
