from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .select(
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.when(F.col("t.is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("account_id")
)
