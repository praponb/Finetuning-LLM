from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("accounts.customer_id").isNull())
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.account_id"),
        F.col("accounts.customer_id"),
    )
    .orderBy("account_id")
)
