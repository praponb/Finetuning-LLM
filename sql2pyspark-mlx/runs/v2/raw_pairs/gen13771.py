from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant") != "Shell").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id").isin([19, 25, 34, 38]))
    .select(F.col("a.id"), F.col("a.account_type"))
    .orderBy("account_type")
)
