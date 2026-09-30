from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.customer_id").isin([3, 6, 12, 31]))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("accounts.account_type"),
    )
    .orderBy(F.col("id").desc())
)
