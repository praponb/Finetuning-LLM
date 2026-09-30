from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_type"),
        F.col("transactions.amount"),
        F.col("transactions.txn_time"),
    )
    .orderBy("txn_time", F.col("txn_type").desc(), "id")
    .limit(50)
)
