from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance").isNotNull()).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.amount") <= 1600)
    .select(F.col("t.id"), F.col("t.amount"))
)
