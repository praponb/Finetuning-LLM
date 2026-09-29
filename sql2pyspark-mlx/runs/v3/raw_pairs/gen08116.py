from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.age"),
        F.coalesce(F.col("c.segment"), F.lit("N/A")).alias("segment_filled"),
    )
    .orderBy("status", "segment_filled")
)
