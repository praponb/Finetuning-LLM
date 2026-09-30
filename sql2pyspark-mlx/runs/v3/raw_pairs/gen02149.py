from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .filter(F.col("accounts.account_type").isin(["SAVINGS", "LOAN"]))
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.concat_ws(",", F.col("accounts.status"), F.col("accounts.account_type")).alias("status_account_type_key"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
