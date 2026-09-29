from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.count(F.col("transactions.amount")).alias("amount_count"),
        F.sum(F.col("transactions.amount")).alias("total_amount"),
    )
    .orderBy("amount_count", "is_active")
    .limit(10)
)
