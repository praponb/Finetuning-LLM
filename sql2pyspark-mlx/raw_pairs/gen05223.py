from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant").isin(["Amazon", "Starbucks", "7-Eleven"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("a.balance"))
    .orderBy("customer_id", F.col("id").desc())
)
