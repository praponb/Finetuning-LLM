from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.balance") >= 79000)
    .select(F.col("a.id"), F.col("a.status"), F.col("a.customer_id"))
    .orderBy("id")
    .limit(20)
)
