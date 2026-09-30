from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.customer_id").isNotNull() | F.col("accounts.customer_id").isNotNull())
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("transactions.is_flagged"),
        F.col("transactions.merchant"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
    )
    .orderBy(F.col("balance").desc())
)
