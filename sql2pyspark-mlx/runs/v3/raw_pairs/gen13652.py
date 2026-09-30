from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("c.is_active"),
        F.col("t.is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("is_flagged_flag")
)
