from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .groupBy(F.col("accounts.account_type"))
    .agg(
        (F.max(F.col("transactions.amount")) - F.min(F.col("transactions.amount"))).alias("amount_range"),
        F.count("*").alias("num_transactions"),
    )
)
