from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.year("opened_date") == 2025).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.txn_time") < "2024-09-01")
    .select(F.col("t.id"), F.col("t.txn_time"), F.col("t.account_id"))
)
