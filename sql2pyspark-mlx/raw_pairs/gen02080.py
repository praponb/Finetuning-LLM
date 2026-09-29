from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.filter(F.col("customer_id").isin([3, 18])).alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left_anti")
    .filter(F.col("transactions.amount") >= 3400)
    .select(
        F.col("transactions.id"),
        F.col("transactions.txn_time"),
        F.col("transactions.txn_type"),
    )
    .orderBy("id", "txn_time")
)
