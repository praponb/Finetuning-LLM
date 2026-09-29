from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount").between(2400, 3400)).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.status").isNotNull())
    .select(F.col("a.id"), F.col("a.status"))
    .orderBy(F.col("id").desc())
)
