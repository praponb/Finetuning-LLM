from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(~F.col("t.is_flagged"))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.when(F.col("t.is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
