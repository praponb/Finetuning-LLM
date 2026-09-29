from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter((F.col("a.balance") >= 39000) | (F.col("t.txn_time") < "2023-11-16"))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.date_format(F.col("t.txn_time"), "yyyy-MM-dd").alias("txn_time_label"),
    )
    .orderBy("txn_time")
)
