from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "REFUND").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("a.balance"),
    )
)
