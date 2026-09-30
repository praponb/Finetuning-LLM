from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.hour("txn_time").between(11, 19)).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
    )
    .orderBy("account_type")
)
