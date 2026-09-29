from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id") == 3)
    .select(
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
    )
    .orderBy("merchant", "customer_id")
)
