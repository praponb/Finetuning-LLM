from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.balance") < 69000)
    .groupBy(F.col("a.customer_id"))
    .agg(F.count(F.when(F.col("t.merchant") == "Lazada", 1)).alias("lazada_count"))
    .orderBy(F.col("lazada_count").desc(), F.col("customer_id").desc())
    .limit(10)
)
