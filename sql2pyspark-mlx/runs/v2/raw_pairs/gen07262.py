from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(~F.col("is_flagged")).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.opened_date").between("2023-04-01", "2024-10-01"))
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.status"))
)
