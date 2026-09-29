from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.is_flagged"))
    .select(F.col("a.balance"), F.col("a.opened_date"), F.col("t.txn_time"))
    .orderBy("txn_time", F.col("opened_date").desc())
)
