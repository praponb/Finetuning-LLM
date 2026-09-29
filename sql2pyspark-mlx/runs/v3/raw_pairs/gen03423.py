from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("t.merchant"), F.col("t.account_id"))
    .orderBy(F.col("account_id").desc())
)
