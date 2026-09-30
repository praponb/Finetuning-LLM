from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.merchant") == "Shell")
    .select(F.col("a.customer_id"), F.col("a.status"), F.col("t.is_flagged"))
    .orderBy(F.col("status").desc())
)
