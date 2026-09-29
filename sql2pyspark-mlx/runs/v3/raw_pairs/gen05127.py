from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("accounts.customer_id").isNull())
    .select(
        F.col("transactions.account_id"),
        F.col("transactions.merchant"),
        F.col("transactions.is_flagged"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
    )
    .orderBy("account_id")
)
