from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("c.is_active"),
        F.substring(F.col("t.merchant"), 1, 2).alias("merchant_prefix"),
    )
    .orderBy("opened_date")
)
