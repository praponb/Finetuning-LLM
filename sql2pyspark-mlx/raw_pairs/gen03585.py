from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.coalesce(F.col("a.status"), F.lit("N/A")).alias("status_filled"),
    )
)
