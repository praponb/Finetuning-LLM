from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
    .select(F.col("a.status"), F.col("a.opened_date"), F.col("t.txn_time"))
)
