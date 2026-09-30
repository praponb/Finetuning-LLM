from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.opened_date").between("2023-06-11", "2024-10-01"))
    .select(F.col("a.account_type"), F.col("a.status"), F.col("t.amount"))
)
