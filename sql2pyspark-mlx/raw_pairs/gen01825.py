from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("account_type").isin(["LOAN", "SAVINGS"])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.txn_time") < "2023-03-26")
    .select(F.col("t.id"), F.col("t.is_flagged"), F.col("t.merchant"))
)
