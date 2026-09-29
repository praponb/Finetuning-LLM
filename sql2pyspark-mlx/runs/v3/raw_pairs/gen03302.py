from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status").isin(["FROZEN", "CLOSED"])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(~F.col("t.is_flagged"))
    .select(F.col("t.id"), F.col("t.is_flagged"))
    .orderBy("id")
    .limit(3)
)
