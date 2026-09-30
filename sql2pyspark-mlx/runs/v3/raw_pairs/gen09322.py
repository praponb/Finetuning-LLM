from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant") != "7-Eleven")
    .select(F.col("t.id"), F.col("t.is_flagged"), F.col("a.status"), F.col("a.balance"))
    .orderBy("balance")
)
