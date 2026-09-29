from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .select(
        F.col("transactions.account_id"),
        F.col("transactions.amount"),
        F.col("transactions.merchant"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
    )
    .orderBy("opened_date")
)
