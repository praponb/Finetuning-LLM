from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant") != "Amazon").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.opened_date").between("2023-06-14", "2024-04-15"))
    .select(F.col("a.id"), F.col("a.customer_id"))
    .orderBy(F.col("id").desc())
)
