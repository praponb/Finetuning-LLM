from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.status"))
    .orderBy("status", "id")
    .limit(10)
)
