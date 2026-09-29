from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .filter(F.col("accounts.opened_date").between("2024-08-09", "2025-07-13"))
    .select(F.col("accounts.id"), F.col("accounts.balance"))
    .orderBy(F.col("id").desc())
)
