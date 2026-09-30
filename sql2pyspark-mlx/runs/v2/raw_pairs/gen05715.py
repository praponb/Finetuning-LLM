from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.year(F.col("a.opened_date")) >= 2025)
    .select(
        F.col("t.txn_time"),
        F.col("t.account_id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("t.is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("customer_id").desc())
)
