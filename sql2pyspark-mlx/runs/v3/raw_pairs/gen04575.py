from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("opened_date") <= "2023-07-01").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.to_date(F.col("t.txn_time")) < "2025-04-01")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.merchant"),
    )
)
