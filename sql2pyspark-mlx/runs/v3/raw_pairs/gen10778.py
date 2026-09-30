from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.max(F.col("t.txn_time")).alias("latest_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
    )
    .filter(F.col("pct_starbucks") > 62)
)
