from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.balance") > 48000)
    .groupBy(
        F.when(F.col("c.segment").isin(["Corporate", "SMB"]), "tier_1").otherwise("other").alias("segment_group"),
        F.col("c.is_active"),
    )
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.sum(F.when(F.col("t.txn_type") == "REFUND", F.col("t.amount")).otherwise(0)).alias("refund_amount"),
    )
    .orderBy("max_amount")
)
