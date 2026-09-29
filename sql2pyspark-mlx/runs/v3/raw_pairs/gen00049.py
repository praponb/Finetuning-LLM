from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("account_id").isin([32, 37, 38])).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .select(F.col("accounts.id"), F.col("accounts.status"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
