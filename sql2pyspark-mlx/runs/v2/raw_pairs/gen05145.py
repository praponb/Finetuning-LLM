from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.customer_id").isNull() & ~F.col("transactions.is_flagged"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("transactions.amount"),
        F.col("transactions.is_flagged"),
        F.col("accounts.status"),
    )
    .orderBy("amount", "id")
)
