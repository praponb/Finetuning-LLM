from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.account_id").isin([2, 7, 21, 40]))
    .select(F.col("a.balance"), F.col("a.account_type"), F.col("t.amount"))
    .orderBy("balance")
)
