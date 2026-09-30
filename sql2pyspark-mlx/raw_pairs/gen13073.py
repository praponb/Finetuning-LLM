from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.customer_id").isNull())
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.lower(F.col("accounts.account_type")).alias("account_type_lower"),
    )
    .orderBy("account_type_lower", "account_type", "id")
    .limit(1)
)
