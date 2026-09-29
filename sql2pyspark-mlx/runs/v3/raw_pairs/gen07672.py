from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .filter(~F.col("accounts.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
    )
    .orderBy("balance")
)
