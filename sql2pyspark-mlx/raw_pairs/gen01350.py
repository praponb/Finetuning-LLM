from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("transactions.account_id").isin([9, 13]))
    .select(
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("transactions.amount"),
    )
)
