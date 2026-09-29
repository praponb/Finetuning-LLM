from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_time") < "2024-07-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
    )
)
