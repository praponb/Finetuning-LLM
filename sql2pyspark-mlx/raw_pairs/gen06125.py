from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("is_flagged")).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .filter(F.col("accounts.customer_id").isin([2, 23, 38]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
    )
)
