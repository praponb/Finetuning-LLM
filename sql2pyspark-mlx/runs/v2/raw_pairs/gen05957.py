from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .orderBy("opened_year", F.col("refund_count").desc())
)
