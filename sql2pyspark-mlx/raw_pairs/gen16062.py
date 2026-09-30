from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.merchant").isin(["Shell", "Amazon", "Lazada"]) & (F.col("a.account_type") == "CREDIT"))
    .select(F.col("a.opened_date"), F.col("t.txn_time"))
)
