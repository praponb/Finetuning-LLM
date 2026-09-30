from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([13, 16, 17, 32])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull())
    .select(F.col("a.id"), F.col("a.status"))
)
