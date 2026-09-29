from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.status") == "CLOSED")
    .select(F.col("a.id"), F.col("a.status"))
    .orderBy("status", F.col("id").desc())
    .limit(10)
)
