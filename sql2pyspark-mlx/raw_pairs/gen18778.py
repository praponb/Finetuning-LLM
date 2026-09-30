from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([1, 29])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.opened_date").between("2023-11-01", "2024-10-01"))
    .select(F.col("a.id"), F.col("a.status"), F.col("a.opened_date"))
)
