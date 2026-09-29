from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("opened_date") <= "2024-09-01").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.merchant") == "Lazada")
    .select(F.col("t.id"), F.col("t.amount"), F.col("t.txn_time"))
    .orderBy("amount", "id")
    .limit(10)
)
