from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.status") == "OPEN")
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
    )
)
