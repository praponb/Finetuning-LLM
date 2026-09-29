from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter((F.col("t.account_id") == 37) | (F.col("a.status") == "OPEN"))
    .select(F.col("t.merchant"), F.col("a.customer_id"))
)
