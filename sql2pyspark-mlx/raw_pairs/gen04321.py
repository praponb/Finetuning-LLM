from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.opened_date") >= "2024-10-01")
    .select(F.col("t.merchant"), F.col("t.account_id"), F.col("a.status"))
    .orderBy(F.col("status").desc())
)
