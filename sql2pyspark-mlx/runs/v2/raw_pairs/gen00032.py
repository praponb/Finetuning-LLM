from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.status") == "OPEN")
    .select(F.col("a.customer_id"), F.col("t.merchant"))
    .orderBy("merchant", F.col("customer_id").desc())
)
