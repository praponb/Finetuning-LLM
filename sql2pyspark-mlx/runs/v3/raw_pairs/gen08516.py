from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date").between("2024-08-24", "2025-03-01"))
    .select(F.col("a.customer_id"), F.col("a.account_type"), F.col("t.account_id"))
    .orderBy(F.col("account_id").desc())
)
