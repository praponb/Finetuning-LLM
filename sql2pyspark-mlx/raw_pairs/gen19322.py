from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.customer_id").isNull() & F.col("accounts.customer_id").isNull())
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("accounts.account_type"),
    )
    .orderBy(F.col("account_type").desc())
)
