from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.status") == "OPEN")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.account_type"),
    )
)
