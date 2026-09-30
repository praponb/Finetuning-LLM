from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.merchant").isin(["Amazon", "7-Eleven", "Lazada"]) & F.col("t.is_flagged"))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.customer_id"),
    )
    .orderBy("txn_type")
)
