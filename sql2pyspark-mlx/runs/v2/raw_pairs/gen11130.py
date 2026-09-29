from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("t.txn_time") < "2025-01-01") & (F.to_date(F.col("t.txn_time")) < "2023-12-01"))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_date_label"),
    )
)
