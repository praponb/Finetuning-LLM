from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("is_flagged")).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .filter(F.col("accounts.balance").between(65000, 73000))
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
    )
)
