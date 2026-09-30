from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isNotNull() & (F.col("t.merchant") == "Starbucks"))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.txn_time"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.is_active"),
        F.coalesce(F.col("c.segment"), F.lit("Unknown")).alias("segment_filled"),
    )
    .orderBy("balance", "id")
    .limit(20)
)
