from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("transactions.amount"),
        F.col("transactions.merchant"),
        F.col("transactions.account_id"),
        F.when(F.col("transactions.merchant").isin(["Lazada", "7-Eleven"]), "Tier 1").otherwise("Other").alias("merchant_group"),
    )
)
