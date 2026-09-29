from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(F.col("a.balance"), F.col("a.customer_id"), F.col("t.is_flagged"))
    .orderBy(F.col("customer_id").desc())
)
