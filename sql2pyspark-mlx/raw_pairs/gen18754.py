from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("merchant") != "7-Eleven").alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
    )
    .orderBy("id")
)
