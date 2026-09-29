from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("t.amount") >= 4000) & (F.col("t.amount") <= 2900))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
    .orderBy("balance")
)
