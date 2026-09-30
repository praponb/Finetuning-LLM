from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("is_flagged")).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.balance"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc(), F.col("balance").desc())
)
