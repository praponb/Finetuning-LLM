from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.is_flagged") & F.col("a.status").isin(["FROZEN", "OPEN"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("t.is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
