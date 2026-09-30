from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant") != "Lazada").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id").isin([11, 14]))
    .select(F.col("a.id"), F.col("a.opened_date"))
)
